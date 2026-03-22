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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.ExceptionUtils;

import io.debezium.connector.mysql.MySqlConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

/** IT tests to cover various newly added tables during capture process. */
public class BinlogSplitMetadataReplicationIT extends MySqlSourceTestBase {
    private static final UniqueDatabase DATABASE =
            new UniqueDatabase(MYSQL_CONTAINER, "binlog_split_metadata", "mysqluser", "mysqlpw");
    private static final DataType DATA_TYPE = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
    private static final RowRowConverter ROW_CONVERTER = RowRowConverter.create(DATA_TYPE);
    private static final Function<RowData, String> FORMATTER =
            x -> ROW_CONVERTER.toExternal(x).toString();
    private static final RowDataDebeziumDeserializeSchema DESERIALIZER =
            RowDataDebeziumDeserializeSchema.newBuilder()
                    .setPhysicalRowType((RowType) DATA_TYPE.getLogicalType())
                    .setResultTypeInfo(
                            InternalTypeInfo.of(TypeConversions.fromDataToLogicalType(DATA_TYPE)))
                    .build();

    @Before
    public void before() throws SQLException {
        DATABASE.createAndInitialize();
    }

    @Test
    public void testBinlogSplitMetadataReplication() throws Exception {
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final String savepointDirectory = temporaryFolder.newFolder().toURI().toString();

        try (JobContext<RowData> jobContext = new JobContext<>(savepointDirectory)) {
            // insert initial records into all three tables
            executeSql(
                    "INSERT INTO a VALUES (1001)",
                    "INSERT INTO b VALUES (2001)",
                    "INSERT INTO c VALUES (3001)");

            assertEqualsInAnyOrder(
                    Arrays.asList("+I[1001]", "+I[2001]"),
                    // snapshot tables "a" and "b"
                    fetchFromSource(jobContext, Arrays.asList("a", "b"), 2));

            // drop table "a", add a binlog change to table "b"
            executeSql("DROP TABLE a", "INSERT INTO b VALUES (2002)");

            Assert.assertEquals(
                    Collections.singletonList("+I[2002]"),
                    // consume the binlog change from table "b"
                    fetchFromSource(jobContext, Arrays.asList("a", "b"), 1));

            Assert.assertEquals(
                    Collections.singletonList("+I[3001]"),
                    // snapshot table "c"
                    fetchFromSource(jobContext, Arrays.asList("a", "b", "c"), 1));

            // add a binlog change to table "c"
            executeSql("INSERT INTO c VALUES (3002)");

            Assert.assertEquals(
                    Collections.singletonList("+I[3002]"),
                    // consume the binlog change from table "c"
                    fetchFromSource(jobContext, Arrays.asList("a", "b", "c"), 1));
        } finally {
            temporaryFolder.delete();
        }
    }

    private List<String> fetchFromSource(
            JobContext<RowData> jobContext, List<String> tableNames, int numberOfRecords)
            throws Exception {
        Configuration configuration = new Configuration();

        jobContext
                .getSavepointPath()
                .ifPresent(
                        path ->
                                configuration.setString(
                                        SavepointConfigOptions.SAVEPOINT_PATH, path));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        MySqlSource<RowData> source =
                MySqlSource.<RowData>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(DATABASE.getDatabaseName())
                        .serverTimeZone("UTC")
                        .tableList(
                                tableNames.stream()
                                        .map(t -> DATABASE.getDatabaseName() + "." + t)
                                        .toArray(String[]::new))
                        .username(DATABASE.getUsername())
                        .password(DATABASE.getPassword())
                        .deserializer(DESERIALIZER)
                        .scanNewlyAddedTableEnabled(true)
                        .build();

        DataStreamSource<RowData> dataStreamSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        TypeSerializer<RowData> serializer =
                dataStreamSource.getType().createSerializer(dataStreamSource.getExecutionConfig());

        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<RowData> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);

        CollectSinkOperator<RowData> operator =
                (CollectSinkOperator<RowData>) factory.getOperator();

        CollectStreamSink<RowData> sink = new CollectStreamSink<>(dataStreamSource, factory);
        dataStreamSource.getExecutionEnvironment().addOperator(sink.getTransformation());

        CollectResultIterator<RowData> iterator =
                jobContext.getIterator(
                        () ->
                                new CollectResultIterator<>(
                                        operator.getOperatorIdFuture(),
                                        serializer,
                                        accumulatorName,
                                        dataStreamSource
                                                .getExecutionEnvironment()
                                                .getCheckpointConfig()));

        JobClient jobClient = env.executeAsync("Collect");
        iterator.setJobClient(jobClient);

        List<String> records = new ArrayList<>(numberOfRecords);
        while (records.size() < numberOfRecords && iterator.hasNext()) {
            records.add(FORMATTER.apply(iterator.next()));
        }

        jobContext.setSavepointPath(
                triggerSavepointWithRetry(jobClient, jobContext.getSavepointDirectory()));

        jobClient.cancel().get();

        return records;
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient
                        .triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    private void executeSql(String... statements) throws SQLException {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", DATABASE.getUsername());
        properties.put("database.password", DATABASE.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);

        try (MySqlConnection connection =
                DebeziumUtils.createMySqlConnection(configuration, new Properties())) {
            connection.execute("USE " + DATABASE.getDatabaseName());
            connection.execute(statements);
        }
    }

    private static class JobContext<T> implements AutoCloseable {
        private final String savepointDirectory;
        private String savepointPath;
        private CollectResultIterator<T> iterator;

        public JobContext(String savepointDirectory) {
            this.savepointDirectory = savepointDirectory;
        }

        public Optional<String> getSavepointPath() {
            return Optional.ofNullable(savepointPath);
        }

        public void setSavepointPath(String savepointPath) {
            this.savepointPath = savepointPath;
        }

        public String getSavepointDirectory() {
            return savepointDirectory;
        }

        public CollectResultIterator<T> getIterator(Supplier<CollectResultIterator<T>> supplier) {
            if (iterator == null) {
                iterator = supplier.get();
            }
            return iterator;
        }

        @Override
        public void close() throws Exception {
            if (iterator != null) {
                iterator.close();
            }
        }
    }
}
