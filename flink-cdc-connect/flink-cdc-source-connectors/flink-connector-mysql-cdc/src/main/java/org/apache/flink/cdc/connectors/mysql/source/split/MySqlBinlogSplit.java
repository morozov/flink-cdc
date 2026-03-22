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

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** The split to describe the binlog of MySql table(s). */
public class MySqlBinlogSplit extends MySqlSplit {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplit.class);
    private static final int TABLES_LENGTH_FOR_LOG = 3;

    private final BinlogOffset startingOffset;
    private final BinlogOffset endingOffset;

    /** Split IDs of all elements must be unique. */
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;

    private final Map<TableId, TableChange> tableSchemas;

    /**
     * The digest of the binlog split metadata that currently exist on the enumerator.
     *
     * <p>Once all finished snapshot split infos have been replicated, their checksum is validated
     * against the expected one.
     */
    private final Digest digest;

    private final boolean isSuspended;
    private final String tablesForLog;
    @Nullable transient byte[] serializedFormCache;

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            Digest digest,
            boolean isSuspended) {
        super(splitId);

        ensureNoDuplicates(finishedSnapshotSplitInfos);

        int totalNumberOfFinishedSnapshotSplits = digest.getTotalNumberOfFinishedSnapshotSplits();
        int numberOfReplicatedFinishedSnapshotSplits = finishedSnapshotSplitInfos.size();

        if (numberOfReplicatedFinishedSnapshotSplits > totalNumberOfFinishedSnapshotSplits) {
            throw new IllegalArgumentException(
                    String.format(
                            "The number of replicated finished snapshot split infos %d cannot be larger than the total number %d.",
                            numberOfReplicatedFinishedSnapshotSplits,
                            totalNumberOfFinishedSnapshotSplits));
        }

        // validate the checksum once all metadata has been fully replicated
        if (numberOfReplicatedFinishedSnapshotSplits == totalNumberOfFinishedSnapshotSplits) {
            digest.validate(finishedSnapshotSplitInfos);
        }

        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.digest = digest;
        this.tableSchemas = tableSchemas;
        this.isSuspended = isSuspended;
        this.tablesForLog = getTablesForLog();
    }

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            Digest digest) {
        this(
                splitId,
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                digest,
                false);
    }

    private static void ensureNoDuplicates(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos) {
        Set<String> seenSplitIds = new HashSet<>();
        for (FinishedSnapshotSplitInfo splitInfo : finishedSnapshotSplitInfos) {
            if (seenSplitIds.contains(splitInfo.getSplitId())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Found duplicate split ID %s in finished snapshot split infos",
                                splitInfo.getSplitId()));
            }

            seenSplitIds.add(splitInfo.getSplitId());
        }
    }

    public BinlogOffset getStartingOffset() {
        return startingOffset;
    }

    public BinlogOffset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public Digest getDigest() {
        return digest;
    }

    public boolean isSuspended() {
        return isSuspended;
    }

    public boolean isCompletedSplit() {
        return finishedSnapshotSplitInfos.size() == digest.totalNumberOfFinishedSnapshotSplits;
    }

    private String getTablesForLog() {
        List<TableId> tablesForLog = new ArrayList<>();
        if (tableSchemas != null) {
            List<TableId> tableIds = new ArrayList<>(new TreeSet(tableSchemas.keySet()));
            // Truncate tables length to avoid printing too much log
            tablesForLog = tableIds.subList(0, Math.min(tableIds.size(), TABLES_LENGTH_FOR_LOG));
        }
        return tablesForLog.toString();
    }

    public String getTables() {
        return tablesForLog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlBinlogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MySqlBinlogSplit that = (MySqlBinlogSplit) o;
        return Objects.equals(digest, that.digest)
                && isSuspended == that.isSuspended
                && Objects.equals(startingOffset, that.startingOffset)
                && Objects.equals(endingOffset, that.endingOffset)
                && Objects.equals(finishedSnapshotSplitInfos, that.finishedSnapshotSplitInfos)
                && Objects.equals(tableSchemas, that.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                digest,
                isSuspended);
    }

    @Override
    public String toString() {
        return "MySqlBinlogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", tables="
                + tablesForLog
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + ", digest="
                + digest
                + '}';
    }

    // -------------------------------------------------------------------
    // factory utils to build new MySqlBinlogSplit instance
    // -------------------------------------------------------------------
    public static MySqlBinlogSplit appendFinishedSplitInfos(
            MySqlBinlogSplit binlogSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        // re-calculate the starting binlog offset after the new table added
        BinlogOffset startingOffset = binlogSplit.getStartingOffset();
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (splitInfo.getHighWatermark().isBefore(startingOffset)) {
                startingOffset = splitInfo.getHighWatermark();
            }
        }

        List<FinishedSnapshotSplitInfo> updatedSplitInfos =
                new ArrayList<>(binlogSplit.getFinishedSnapshotSplitInfos());
        updatedSplitInfos.addAll(splitInfos);

        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                startingOffset,
                binlogSplit.getEndingOffset(),
                updatedSplitInfos,
                binlogSplit.getTableSchemas(),
                binlogSplit.getDigest(),
                binlogSplit.isSuspended());
    }

    public static MySqlBinlogSplit replaceFinishedSplitInfos(
            MySqlBinlogSplit binlogSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        LOG.info("Creating new binlogsplit with the new table splits");
        // added by decodable in case where we want to fully replace the splitinfos of the binlog
        // this should be similar to appendFinishedSplitInfos, only replacing the splitinfos but
        // everything else same
        // re-calculate the starting binlog offset after the new table added

        // find new splits that did not exist
        Set<String> oldExistingSplitIds =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .map(FinishedSnapshotSplitInfo::getSplitId)
                        .collect(Collectors.toSet());
        List<FinishedSnapshotSplitInfo> newSplitsOnly = new ArrayList<>();
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (!oldExistingSplitIds.contains(splitInfo.getSplitId())) {
                newSplitsOnly.add(splitInfo);
            }
        }
        LOG.info(
                "New splits extracted: "
                        + newSplitsOnly.stream()
                                .map(FinishedSnapshotSplitInfo::getSplitId)
                                .collect(Collectors.toList()));

        BinlogOffset newSplitsStartingOffset = null;
        FinishedSnapshotSplitInfo lowestHMSplit = null;
        for (FinishedSnapshotSplitInfo newSplit : newSplitsOnly) {
            // calculate the starting offset based on new splits only (lowest of the highwatermark)
            if (newSplitsStartingOffset == null) {
                newSplitsStartingOffset = newSplit.getHighWatermark();
                lowestHMSplit = newSplit;
            } else if (newSplit.getHighWatermark().isBefore(newSplitsStartingOffset)) {
                newSplitsStartingOffset = newSplit.getHighWatermark();
                lowestHMSplit = newSplit;
            }
        }

        // take the earliest of either the new table splits's starting offset or the last binlog
        // offset read
        BinlogOffset lastBinlogOffset = binlogSplit.getStartingOffset();
        BinlogOffset startingOffset = newSplitsStartingOffset;
        if (lastBinlogOffset.isBefore(newSplitsStartingOffset)) {
            startingOffset = lastBinlogOffset;
            LOG.info(
                    "New binlog starting offset was set from last binlog offset position "
                            + startingOffset);
        } else {
            LOG.info(
                    "New binlog starting offset was set from new table: "
                            + lowestHMSplit
                            + ",  starting offset: "
                            + startingOffset);
        }

        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                startingOffset,
                binlogSplit.getEndingOffset(),
                splitInfos,
                binlogSplit.getTableSchemas(),
                binlogSplit.getDigest(),
                binlogSplit.isSuspended());
    }

    /**
     * Constructs a new instance derived from this one without the no longer relevant table schemas.
     *
     * <p>Once the source configuration has changed, the binlog split may contain the schemas of the
     * tables that are no longer captured by the source. Their schemas are no longer relevant and
     * may be discarded.
     */
    public MySqlBinlogSplit withoutIrrelevantTableSchemas(Tables.TableFilter tableFilter) {
        Map<TableId, TableChange> relevantTableSchemas =
                tableSchemas.entrySet().stream()
                        .filter(entry -> tableFilter.isIncluded(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (relevantTableSchemas.size() == tableSchemas.size()) {
            return this;
        }

        return new MySqlBinlogSplit(
                splitId,
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                relevantTableSchemas,
                digest,
                isSuspended);
    }

    public static MySqlBinlogSplit fillTableSchemas(
            MySqlBinlogSplit binlogSplit, Map<TableId, TableChange> tableSchemas) {
        tableSchemas.putAll(binlogSplit.getTableSchemas());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                binlogSplit.getFinishedSnapshotSplitInfos(),
                tableSchemas,
                binlogSplit.getDigest(),
                binlogSplit.isSuspended());
    }

    /**
     * Constructs a new instance derived from this one with the given metadata digest. The {@link
     * #finishedSnapshotSplitInfos} will be cleared on the new instance in order to initialize their
     * replication from scratch.
     */
    public MySqlBinlogSplit withNewDigest(MySqlBinlogSplit.Digest digest) {
        return new MySqlBinlogSplit(
                splitId,
                startingOffset,
                endingOffset,
                Collections.emptyList(),
                tableSchemas,
                digest,
                false);
    }

    public static MySqlBinlogSplit toSuspendedBinlogSplit(MySqlBinlogSplit normalBinlogSplit) {
        return new MySqlBinlogSplit(
                normalBinlogSplit.splitId,
                normalBinlogSplit.getStartingOffset(),
                normalBinlogSplit.getEndingOffset(),
                Collections.emptyList(),
                normalBinlogSplit.getTableSchemas(),
                Digest.empty(),
                true);
    }

    /** Represents the digest of the binlog split metadata. */
    public static class Digest implements Serializable {
        private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplit.Digest.class);

        /**
         * The total number of finished snapshot splits that need to be replicated to the source
         * reader before the binlog split is considered complete.
         */
        private final int totalNumberOfFinishedSnapshotSplits;

        /**
         * The checksum that the finished snapshot split infos must have once fully replicated to
         * the source reader.
         */
        private final int checksum;

        /**
         * This constructor exists primarily for use during deserialization. In all other cases, use
         * the static factory methods provided.
         */
        public Digest(int totalNumberOfFinishedSnapshotSplits, int checksum) {
            if (totalNumberOfFinishedSnapshotSplits < 0) {
                throw new IllegalArgumentException(
                        "The total number of finished snapshot splits cannot be negative.");
            }

            this.totalNumberOfFinishedSnapshotSplits = totalNumberOfFinishedSnapshotSplits;
            this.checksum = checksum;
        }

        public int getTotalNumberOfFinishedSnapshotSplits() {
            return totalNumberOfFinishedSnapshotSplits;
        }

        public int getChecksum() {
            return checksum;
        }

        public void validate(List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos) {
            int actualChecksum = checksum(finishedSnapshotSplitInfos);
            if (actualChecksum != checksum) {
                throw new IllegalArgumentException(
                        String.format(
                                "The checksum of finished snapshot split infos %d does not match the expected checksum %d.",
                                actualChecksum, checksum));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Digest)) {
                return false;
            }
            Digest that = (Digest) o;
            return checksum == that.checksum
                    && totalNumberOfFinishedSnapshotSplits
                            == that.totalNumberOfFinishedSnapshotSplits;
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalNumberOfFinishedSnapshotSplits, checksum);
        }

        @Override
        public String toString() {
            return "Digest{"
                    + "totalNumberOfFinishedSnapshotSplits="
                    + totalNumberOfFinishedSnapshotSplits
                    + ", checksum="
                    + checksum
                    + '}';
        }

        /** Creates a checksum based on the list of finished snapshot split infos. */
        public static Digest of(List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos) {
            return new Digest(
                    finishedSnapshotSplitInfos.size(), checksum(finishedSnapshotSplitInfos));
        }

        /** Creates an empty digest. */
        public static Digest empty() {
            return of(Collections.emptyList());
        }

        /** Calculates the checksum of the given finished snapshot split infos. */
        private static int checksum(List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos) {
            int checksum = finishedSnapshotSplitInfos.hashCode();
            LOG.debug("Calculated checksum of {}: {}", finishedSnapshotSplitInfos, checksum);
            return checksum;
        }
    }
}
