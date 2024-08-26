/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.ververica.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import com.ververica.cdc.connectors.mysql.source.utils.StatementUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPositionWithoutServerId;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;

/**
 * A Debezium binlog reader implementation that also support reads binlog and filter overlapping
 * snapshot data that {@link SnapshotSplitReader} read.
 */
public class BinlogSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executorService;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    private MySqlBinlogSplitReadTask binlogSplitReadTask;
    private MySqlBinlogSplit currentBinlogSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, BinlogOffset> maxSplitHighWatermarkMap;
    // max of all splits high watermark. Used as limit for scanning for deletes.
    private BinlogOffset maxAllSplitsHighWatermark;
    private final Set<TableId> pureBinlogPhaseTables;
    private Tables.TableFilter capturedTableFilter;
    private final StoppableChangeEventSourceContext changeEventSourceContext =
            new StoppableChangeEventSourceContext();
    private boolean scanDanglingDeletes;
    // map to track # of records emitted
    private final ScheduledExecutorService metricsLoggerScheduler;
    private Map<String, Map<String, AtomicInteger>> recordsEmittedMap = new HashMap<>();

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subTaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("binlog-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureBinlogPhaseTables = new HashSet<>();
        this.scanDanglingDeletes =
                !this.statefulTaskContext
                        .getSourceReaderContext()
                        .isStartedWithAssignedBinlogSplit();
        metricsLoggerScheduler = schedulePeriodicMetricsLogging();
    }

    private ScheduledExecutorService schedulePeriodicMetricsLogging() {
        // schedule a reporter task to log # of records emitted per table
        // we probably want to remove the log once we know drata is good
        // this is helpful in debugging their missing data issue right now
        ScheduledExecutorService metricsLoggerScheduler =
                Executors.newScheduledThreadPool(
                        1,
                        r -> {
                            Thread t = new Thread(r, "metrics-logger");
                            t.setDaemon(true);
                            return t;
                        });
        metricsLoggerScheduler.scheduleAtFixedRate(
                () -> {
                    if (!currentTaskRunning) {
                        LOG.info(
                                "Skipping logging stats since current binlog-read task is not running");
                        return;
                    }

                    Map<String, Map<String, AtomicInteger>> oldRecordsEmittedMap =
                            recordsEmittedMap;
                    recordsEmittedMap = new HashMap<>();
                    LOG.info("Records emitted since last:");
                    for (Map.Entry<String, Map<String, AtomicInteger>> tableIdOpCounterMap :
                            oldRecordsEmittedMap.entrySet()) {
                        String tableId = tableIdOpCounterMap.getKey();
                        // iterate over record's op specific metrics
                        for (Map.Entry<String, AtomicInteger> e :
                                tableIdOpCounterMap.getValue().entrySet()) {
                            String op = e.getKey();
                            LOG.info(
                                    "Records emitted: "
                                            + tableId
                                            + ",  "
                                            + op
                                            + ", "
                                            + e.getValue().intValue());
                        }
                    }

                    // detect any tables missing from binlog reading
                    Set<String> expectedTables =
                            new HashSet<>(statefulTaskContext.getSourceConfig().getTableList());
                    LOG.info("Expected tables in binlog reading count: " + expectedTables.size());
                    for (TableId tableId : finishedSplitsInfo.keySet()) {
                        expectedTables.remove(tableId.identifier());
                    }
                    if (expectedTables.isEmpty()) {
                        LOG.info("All expected tables are being read from binlog.");
                    } else {
                        LOG.error(
                                "Following expected tables are not being read from binlog "
                                        + expectedTables);
                    }
                },
                5,
                5,
                TimeUnit.MINUTES);
        return metricsLoggerScheduler;
    }

    public void submitSplit(MySqlSplit mySqlSplit) {
        this.currentBinlogSplit = mySqlSplit.asBinlogSplit();
        configureFilter();
        statefulTaskContext.configure(currentBinlogSplit);
        this.capturedTableFilter =
                statefulTaskContext.getConnectorConfig().getTableFilters().dataCollectionFilter();
        this.queue = statefulTaskContext.getQueue();
        this.binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getSignalEventDispatcher(),
                        statefulTaskContext.getErrorHandler(),
                        StatefulTaskContext.getClock(),
                        statefulTaskContext.getTaskContext(),
                        (MySqlStreamingChangeEventSourceMetrics)
                                statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                        currentBinlogSplit,
                        createEventFilter());

        executorService.submit(
                () -> {
                    try {
                        binlogSplitReadTask.execute(
                                changeEventSourceContext,
                                statefulTaskContext.getMySqlPartition(),
                                statefulTaskContext.getOffsetContext());
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for mysql split %s fail",
                                        currentBinlogSplit),
                                e);
                        readException = e;
                    } finally {
                        stopBinlogReadTask();
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentBinlogSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            if (scanDanglingDeletes) {
                checkForDanglingDeletes(batch, sourceRecords); // first process all dangling deletes
            }
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
            List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return null;
        }
    }

    private void checkForDanglingDeletes(
            List<DataChangeEvent> batch, List<SourceRecord> sourceRecords) {
        if (batch.isEmpty()) {
            return;
        }

        LOG.info("Check for dangling delete - batch start");
        // if there is a force restart with existing binlog filename and position set, then we want
        // to check for deletes for all tracked table splits until each of their new startoffset is
        // hit in case we missed the deletes in between. this patch was added to operationally
        // address the dangling deletes issue for drata so that we can pickup delete events that
        // happen during some downtime between connection stop and force restart.

        Map<Struct, SourceRecord> deletes = new HashMap<>();
        boolean binlogReaderOnlyPositionReached =
                readBatchForDeletesUntilCurrentBinlogPosition(batch, deletes);
        if (binlogReaderOnlyPositionReached) {
            // when this is done - there should be no further check deletes since we looked at
            // delete event for all tables until their high watermark. Normal binlog processing
            // can handle the rest of the deletes too now.
            scanDanglingDeletes = false;
            LOG.info("Check for dangling delete - stop checking for further dangling deletes");
        }

        removeDeletesThatCurrentlyExists(deletes);

        if (!deletes.isEmpty()) {
            // LOG delete count for tables
            Map<TableId, AtomicInteger> tablesDeletesCountMap = new HashMap<>();
            for (Map.Entry<Struct, SourceRecord> entry : deletes.entrySet()) {
                TableId tableId = getTableId(entry.getValue());
                tablesDeletesCountMap
                        .computeIfAbsent(tableId, counter -> new AtomicInteger(0))
                        .incrementAndGet();
            }
            for (Map.Entry<TableId, AtomicInteger> entry : tablesDeletesCountMap.entrySet()) {
                LOG.info(
                        "Check for dangling delete - issuing deletes for "
                                + entry.getKey()
                                + ", count: "
                                + entry.getValue().intValue());
            }
        }

        sourceRecords.addAll(deletes.values());
        if (!scanDanglingDeletes) {
            LOG.info("Check for dangling delete - fully complete");
        } else {
            LOG.info(
                    "Check for dangling delete - partially complete, will wait for more events from binlog");
        }
    }

    /**
     * @param batch - current cdc record batch from the last poll
     * @param deletes - map to add any deletes that is discovered
     * @return true if last offset for dangling deletes found false to continue scanning for
     *     dangling deletes
     */
    private boolean readBatchForDeletesUntilCurrentBinlogPosition(
            List<DataChangeEvent> batch, Map<Struct, SourceRecord> deletes) {
        if (maxAllSplitsHighWatermark == null) {
            maxAllSplitsHighWatermark = currentBinlogSplit.getStartingOffset();
        }

        for (DataChangeEvent dataChangeEvent : batch) {
            SourceRecord sourceRecord = dataChangeEvent.getRecord();
            // we get binlog positions without serverid, since we know they are from same server and
            // split watermarks
            // don't have server-id (additionally no timestamp too)
            BinlogOffset binlogPosition =
                    getBinlogPositionWithoutServerId(sourceRecord.sourceOffset());

            if (binlogPosition.isAtOrAfter(maxAllSplitsHighWatermark)) {
                LOG.info(
                        "Check for dangling delete - normal binlog processing offset reached {}",
                        maxAllSplitsHighWatermark);
                // record is at or after the offset that should be normally handled
                // this is end of the delete tracking. We can proceed with just normal processing
                // from this position
                return true;
            }

            if (isDataChangeRecord(sourceRecord)) {
                TableId tableId = getTableId(sourceRecord);
                if (!maxSplitHighWatermarkMap.containsKey(tableId)
                        || binlogPosition.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
                    LOG.info(
                            "Check for dangling delete - skipping non tracked table or "
                                    + "table's high watermark is already reached {}",
                            tableId);
                    // record is not from the table that is tracked and was not newly introduced
                    // OR we reached the high watermark for the specific table.
                    // move on to the next record
                } else { // only process if the record is before the highwatermark for the specific
                    // table
                    String op =
                            ((Struct) sourceRecord.value()).getString(Envelope.FieldName.OPERATION);
                    Struct key = (Struct) sourceRecord.key();
                    if (op.equals("d")) {
                        deletes.put(key, sourceRecord);
                    }
                }
            }
        }
        return false;
    }

    private void removeDeletesThatCurrentlyExists(Map<Struct, SourceRecord> deletes) {
        LOG.info("Check for dangling delete - check if deletes exists as new row in the db");
        List<Struct> deletesToRemove = new ArrayList<>();
        for (Map.Entry<Struct, SourceRecord> entry : deletes.entrySet()) {
            Struct rowKey = entry.getKey();
            Schema keySchema = rowKey.schema();
            Object[] recordPKVals = new Object[keySchema.fields().size()];
            for (Field keySchemaField : keySchema.fields()) {
                Object value = rowKey.get(keySchemaField);
                recordPKVals[keySchemaField.index()] = value;
            }

            TableId tableId = getTableId(entry.getValue());
            Table table = this.statefulTaskContext.getDatabaseSchema().tableFor(tableId);
            if (table == null) {
                // this should not happen since we already filter it out during reading the events
                // and qualifying them
                throw new RuntimeException(
                        String.format(
                                "Cannot issue delete for table %s in this connection, this should not have happened!",
                                tableId));
            }
            if (isRowExists(table, statefulTaskContext.getConnection(), recordPKVals)) {
                deletesToRemove.add(rowKey);
            }
        }
        if (!deletesToRemove.isEmpty()) {
            LOG.info("Check for dangling delete - {} deletes exists in db", deletesToRemove.size());
        } else {
            LOG.info("Check for dangling delete - no deletes exists in db");
        }

        LOG.info("Check for dangling delete - {} deletes before pruning", deletes.size());
        for (Struct deleteToRemoveKey : deletesToRemove) {
            deletes.remove(deleteToRemoveKey);
        }
        LOG.info("Check for dangling delete - {} deletes after pruning", deletes.size());
    }

    private boolean isRowExists(
            Table table, JdbcConnection jdbcConnection, Object[] primaryKeyValues) {
        TableId tableId = table.id();
        List<Column> columns = new ArrayList<>(table.primaryKeyColumns());
        columns.sort(Comparator.comparingInt(Column::position));
        int numRows = 1;
        final String selectSql = StatementUtils.buildRowExistenceQuery(tableId, columns, numRows);

        try (PreparedStatement selectStatement =
                        StatementUtils.readRowExistenceStatement(
                                jdbcConnection,
                                selectSql,
                                Collections.singletonList(primaryKeyValues));
                ResultSet rs = selectStatement.executeQuery()) {
            return rs.next();
        } catch (SQLException e) {
            throw new ConnectException("Row existence check errored on table " + table.id(), e);
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentBinlogSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            if (statefulTaskContext.getConnection() != null) {
                statefulTaskContext.getConnection().close();
            }
            if (statefulTaskContext.getBinaryLogClient() != null) {
                statefulTaskContext.getBinaryLogClient().disconnect();
            }

            stopBinlogReadTask();
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the binlog split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
            statefulTaskContext.getDatabaseSchema().close();
            metricsLoggerScheduler.shutdownNow();
        } catch (Exception e) {
            LOG.error("Close binlog reader error", e);
        }
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the binlog split reader only sends the binlog event that
     * belongs to its finished snapshot splits. For each snapshot split, the binlog event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the binlog event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the binlog event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (RecordUtils.isDataChangeRecord(sourceRecord)) {
            TableId tableId = RecordUtils.getTableId(sourceRecord);
            BinlogOffset position = RecordUtils.getBinlogPosition(sourceRecord);
            if (hasEnterPureBinlogPhase(tableId, position)) {
                trackRowCountForReporting(tableId, sourceRecord);
                return true;
            }

            // only the table who captured snapshot splits need to filter
            if (finishedSplitsInfo.containsKey(tableId)) {
                RowType splitKeyType =
                        ChunkUtils.getChunkKeyColumnType(
                                statefulTaskContext.getDatabaseSchema().tableFor(tableId),
                                statefulTaskContext.getSourceConfig().getChunkKeyColumns());

                Struct target = RecordUtils.getStructContainsChunkKey(sourceRecord);
                Object[] chunkKey =
                        RecordUtils.getSplitKey(
                                splitKeyType, statefulTaskContext.getSchemaNameAdjuster(), target);
                for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                    if (RecordUtils.splitKeyRangeContains(
                                    chunkKey, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                            && position.isAfter(splitInfo.getHighWatermark())) {
                        trackRowCountForReporting(tableId, sourceRecord);
                        return true;
                    }
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        } else if (RecordUtils.isSchemaChangeEvent(sourceRecord)) {
            if (RecordUtils.isTableChangeRecord(sourceRecord)) {
                TableId tableId = RecordUtils.getTableId(sourceRecord);
                return capturedTableFilter.isIncluded(tableId);
            } else {
                // Not related to changes in table structure, like `CREATE/DROP DATABASE`, skip it
                return false;
            }
        }
        // always send the schema change event and signal event
        // we need record them to state of Flink
        // todo: looks like we can also capture schema change event here!!
        return true;
    }

    private void trackRowCountForReporting(TableId tableId, SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        String op;
        if (sourceRecord.valueSchema().field(Envelope.FieldName.OPERATION) == null
                || (op = value.getString(Envelope.FieldName.OPERATION)) == null) {
            op = "no-data-change";
        }

        Map<String, AtomicInteger> tableIdOpCounterMap =
                recordsEmittedMap.computeIfAbsent(tableId.identifier(), s -> new HashMap<>());
        tableIdOpCounterMap.computeIfAbsent(op, ctr -> new AtomicInteger(0)).incrementAndGet();
    }

    private boolean hasEnterPureBinlogPhase(TableId tableId, BinlogOffset position) {
        if (pureBinlogPhaseTables.contains(tableId)) {
            return true;
        }
        // the existed tables those have finished snapshot reading
        // todo: debug and check how isatorafter works since without serverid they dont seem to be
        // working
        // i think there is a bug here:
        // watermarks don't have serverid and timestamp since they are created using master status
        // sql
        // (see: com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.currentBinlogOffset
        // method)
        // when comparing 2 binlogoffset (see:
        // com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset.compareTo)
        // when there are no serverid, comparison is based on timestamp only, bypassing binlog file
        // and position
        // so all read binlog events qualify for emit, instead of just the ones that are at+higher
        // watermark position
        // for append only thing, this probably produces duplicates
        // test: create table A add rows, wait couple seconds, create table B add rows
        // likely observation: snapshot will pick them both and add rows
        // binglog will reprocess table B (since its highwatermark is after the binlog start
        // position - which is the
        // lowest highwater of 2 (ie from table A)
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureBinlogPhaseTables.add(tableId);
            return true;
        }

        // Use still need to capture new sharding table if user disable scan new added table,
        // The history records for all new added tables(including sharding table and normal table)
        // will be capture after restore from a savepoint if user enable scan new added table
        if (!statefulTaskContext.getSourceConfig().isScanNewlyAddedTableEnabled()) {
            // the new added sharding table without history records
            return !maxSplitHighWatermarkMap.containsKey(tableId)
                    && capturedTableFilter.isIncluded(tableId);
        }
        return false;
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentBinlogSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, BinlogOffset> tableIdBinlogPositionMap = new HashMap<>();
        BinlogOffset largestHighWatermark = maxAllSplitsHighWatermark;
        if (largestHighWatermark == null) {
            largestHighWatermark = currentBinlogSplit.getStartingOffset();
        }
        // specific offset mode
        if (finishedSplitInfos.isEmpty()) {
            for (TableId tableId : currentBinlogSplit.getTableSchemas().keySet()) {
                tableIdBinlogPositionMap.put(tableId, currentBinlogSplit.getStartingOffset());
            }
            if (currentBinlogSplit.getStartingOffset().isAfter(largestHighWatermark)) {
                largestHighWatermark = currentBinlogSplit.getStartingOffset();
            }
        }
        // initial mode
        else {
            for (FinishedSnapshotSplitInfo finishedSplitInfo : finishedSplitInfos) {
                TableId tableId = finishedSplitInfo.getTableId();
                List<FinishedSnapshotSplitInfo> list =
                        splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
                list.add(finishedSplitInfo);
                splitsInfoMap.put(tableId, list);

                BinlogOffset highWatermark = finishedSplitInfo.getHighWatermark();
                BinlogOffset maxHighWatermark = tableIdBinlogPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdBinlogPositionMap.put(tableId, highWatermark);
                }
                if (highWatermark.isAfter(largestHighWatermark)) {
                    largestHighWatermark = highWatermark;
                }
            }
        }

        this.maxAllSplitsHighWatermark = largestHighWatermark;
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdBinlogPositionMap;
        this.pureBinlogPhaseTables.clear();
    }

    private Predicate<Event> createEventFilter() {
        // If the startup mode is set as TIMESTAMP, we need to apply a filter on event to drop
        // events earlier than the specified timestamp.

        // NOTE: Here we take user's configuration (statefulTaskContext.getSourceConfig())
        // as the ground truth. This might be fragile if user changes the config and recover
        // the job from savepoint / checkpoint, as there might be conflict between user's config
        // and the state in savepoint / checkpoint. But as we don't promise compatibility of
        // checkpoint after changing the config, this is acceptable for now.
        StartupOptions startupOptions = statefulTaskContext.getSourceConfig().getStartupOptions();
        if (startupOptions.startupMode.equals(StartupMode.TIMESTAMP)) {
            if (startupOptions.binlogOffset == null) {
                throw new NullPointerException(
                        "The startup option was set to TIMESTAMP "
                                + "but unable to find starting binlog offset. Please check if the timestamp is specified in "
                                + "configuration. ");
            }
            long startTimestampSec = startupOptions.binlogOffset.getTimestampSec();
            // We only skip data change event, as other kinds of events are necessary for updating
            // some internal state inside MySqlStreamingChangeEventSource
            LOG.info(
                    "Creating event filter that dropping row mutation events before timestamp in second {}",
                    startTimestampSec);
            return event -> {
                if (!EventType.isRowMutation(getEventType(event))) {
                    return true;
                }
                return event.getHeader().getTimestamp() >= startTimestampSec * 1000;
            };
        }
        return event -> true;
    }

    public void stopBinlogReadTask() {
        currentTaskRunning = false;
        // Terminate the while loop in MySqlStreamingChangeEventSource's execute method
        changeEventSourceContext.stopChangeEventSource();
    }

    private EventType getEventType(Event event) {
        return event.getHeader().getEventType();
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @VisibleForTesting
    MySqlBinlogSplitReadTask getBinlogSplitReadTask() {
        return binlogSplitReadTask;
    }
}
