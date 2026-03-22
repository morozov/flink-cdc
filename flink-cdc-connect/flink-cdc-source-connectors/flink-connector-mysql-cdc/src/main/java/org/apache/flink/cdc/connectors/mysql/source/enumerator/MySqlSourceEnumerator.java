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

package org.apache.flink.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitAssignedEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetadataDigestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetadataDigestRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitUpdateAckEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitUpdateRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    private final SplitEnumeratorContext<MySqlSplit> context;
    private final MySqlSourceConfig sourceConfig;
    private final MySqlSplitAssigner splitAssigner;

    private final Boundedness boundedness;

    // using TreeSet to prefer assigning binlog split to task-0 for easier debug
    private final TreeSet<Integer> readersAwaitingSplit;

    /** The binlog offset for which the current {@link #binlogSplitMetadata} was generated. */
    @Nullable private BinlogOffset latestBinlogOffset;

    /** The metadata of the binlog split to be replicated to the source reader. */
    @Nullable private BinlogSplitMetadata binlogSplitMetadata;

    @Nullable private Integer binlogSplitTaskId;

    private boolean isBinlogSplitUpdateRequestAlreadySent = false;

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSourceConfig sourceConfig,
            MySqlSplitAssigner splitAssigner,
            Boundedness boundedness) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitAssigner = splitAssigner;
        this.boundedness = boundedness;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        splitAssigner.open();
        requestBinlogSplitUpdateIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        LOG.debug("The enumerator adds splits back: {}", splits);
        Optional<MySqlSplit> binlogSplit =
                splits.stream().filter(MySqlSplit::isBinlogSplit).findAny();
        if (binlogSplit.isPresent()) {
            LOG.info("The enumerator adds add binlog split back: {}", binlogSplit);
            this.binlogSplitTaskId = null;
        }
        if (!CollectionUtil.isNullOrEmpty(splits)) {
            splitAssigner.addSplits(splits);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // send BinlogSplitUpdateRequestEvent to source reader after newly added table
        // snapshot splits finished.
        if (isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator under {} receives finished split offsets {} from subtask {}.",
                    splitAssigner.getAssignerStatus(),
                    sourceEvent,
                    subtaskId);
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) sourceEvent;
            Map<String, BinlogOffset> finishedOffsets = reportEvent.getFinishedOffsets();

            splitAssigner.onFinishedSplits(finishedOffsets);
            requestBinlogSplitUpdateIfNeed();

            // send acknowledge event
            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof BinlogSplitMetaRequestEvent) {
            LOG.debug(
                    "The enumerator receives request for binlog split meta from subtask {}.",
                    subtaskId);
            sendBinlogMeta(subtaskId, (BinlogSplitMetaRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof BinlogSplitUpdateAckEvent) {
            LOG.info(
                    "The enumerator receives event that the binlog split has been updated from subtask {}. ",
                    subtaskId);
            splitAssigner.onBinlogSplitUpdated();
        } else if (sourceEvent instanceof BinlogSplitMetadataDigestRequestEvent) {
            LOG.info(
                    "The enumerator receives request for the binlog split metadata digest from subtask {}.",
                    subtaskId);
            handleBinlogSplitMetadataDigestRequest(
                    subtaskId, (BinlogSplitMetadataDigestRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof BinlogSplitAssignedEvent) {
            LOG.info(
                    "The enumerator receives notice from subtask {} for the binlog split assignment. ",
                    subtaskId);
            binlogSplitTaskId = subtaskId;
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // binlog split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            if (shouldCloseIdleReader(nextAwaiting)) {
                // close idle readers when snapshot phase finished.
                context.signalNoMoreSplits(nextAwaiting);
                awaitingReader.remove();
                LOG.info("Close idle reader of subtask {}", nextAwaiting);
                continue;
            }

            Optional<MySqlSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final MySqlSplit mySqlSplit = split.get();
                context.assignSplit(mySqlSplit, nextAwaiting);
                if (mySqlSplit instanceof MySqlBinlogSplit) {
                    this.binlogSplitTaskId = nextAwaiting;
                }
                awaitingReader.remove();
                LOG.info("The enumerator assigns split {} to subtask {}", mySqlSplit, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                requestBinlogSplitUpdateIfNeed();
                break;
            }
        }
    }

    private boolean shouldCloseIdleReader(int nextAwaiting) {
        // When no unassigned split anymore, Signal NoMoreSplitsEvent to awaiting reader in two
        // situations:
        // 1. When Set StartupMode = snapshot mode(also bounded), there's no more splits in the
        // assigner.
        // 2. When set scan.incremental.close-idle-reader.enabled = true, there's no more splits in
        // the assigner.
        return splitAssigner.noMoreSplits()
                && (boundedness == Boundedness.BOUNDED
                        || (sourceConfig.isCloseIdleReaders()
                                && (binlogSplitTaskId != null
                                        && !binlogSplitTaskId.equals(nextAwaiting))));
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(
                        subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }

        requestBinlogSplitUpdateIfNeed();
    }

    private void requestBinlogSplitUpdateIfNeed() {
        if (!isBinlogSplitUpdateRequestAlreadySent
                && isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            for (int subtaskId : getRegisteredReader()) {
                isBinlogSplitUpdateRequestAlreadySent = true;
                LOG.info(
                        "The enumerator requests subtask {} to update the binlog split after newly added table.",
                        subtaskId);
                context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
            }
        }
    }

    /**
     * Returns the current binlog split metadata, generating a new one if the binlog offset changed.
     */
    private BinlogSplitMetadata getBinlogSplitMetadata(BinlogOffset currentBinlogOffset) {
        if (!currentBinlogOffset.equals(latestBinlogOffset)) {
            binlogSplitMetadata =
                    new BinlogSplitMetadata(
                            splitAssigner.getFinishedSplitInfos(),
                            currentBinlogOffset,
                            sourceConfig.getSplitMetaGroupSize());
            latestBinlogOffset = currentBinlogOffset;
        }

        return binlogSplitMetadata;
    }

    private void sendBinlogMeta(int subTask, BinlogSplitMetaRequestEvent requestEvent) {
        BinlogSplitMetadata metadata =
                getBinlogSplitMetadata(requestEvent.getCurrentBinlogOffset());
        final int requestMetaGroupId = requestEvent.getRequestMetaGroupId();
        final MySqlBinlogSplit.Digest expectedDigest = requestEvent.getExpectedDigest();
        final MySqlBinlogSplit.Digest actualDigest = metadata.getDigest();
        if (!expectedDigest.equals(actualDigest)) {
            LOG.info(
                    "The binlog split metadata digest expected by subtask {} is {}, while the digest on the enumerator is {}. Sending the actual metadata digest to the subtask.",
                    subTask,
                    expectedDigest,
                    actualDigest);
            BinlogSplitMetaEvent event =
                    new BinlogSplitMetaEvent(
                            requestEvent.getSplitId(), requestMetaGroupId, null, actualDigest);
            context.sendEventToSourceReader(subTask, event);
            return;
        }

        BinlogSplitMetaEvent metadataEvent =
                new BinlogSplitMetaEvent(
                        requestEvent.getSplitId(),
                        requestMetaGroupId,
                        metadata.getGroup(requestMetaGroupId)
                                .orElseThrow(
                                        () ->
                                                new FlinkRuntimeException(
                                                        String.format(
                                                                "The enumerator received invalid request for binlog split metadata group id %s. The valid metadata group id range is [0, %s]",
                                                                requestMetaGroupId,
                                                                metadata.getNumberOfGroups() - 1)))
                                .stream()
                                .map(FinishedSnapshotSplitInfo::serialize)
                                .collect(Collectors.toList()),
                        actualDigest);
        context.sendEventToSourceReader(subTask, metadataEvent);
    }

    private void handleBinlogSplitMetadataDigestRequest(
            int subTask, BinlogSplitMetadataDigestRequestEvent event) {
        if (splitAssigner instanceof MySqlHybridSplitAssigner) {
            BinlogSplitMetadata metadata = getBinlogSplitMetadata(event.getCurrentBinlogOffset());
            context.sendEventToSourceReader(
                    subTask, new BinlogSplitMetadataDigestEvent(metadata.getDigest()));
        }
    }

    /** The metadata of the binlog split to be replicated to the source reader. */
    static class BinlogSplitMetadata {
        private final List<List<FinishedSnapshotSplitInfo>> groups;
        private final MySqlBinlogSplit.Digest digest;

        public BinlogSplitMetadata(
                List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
                BinlogOffset currentBinlogReadingOffset,
                int groupSize) {
            if (finishedSnapshotSplitInfos.isEmpty()) {
                LOG.error(
                        "The assigner offers empty finished split information, this should not happen");
                throw new FlinkRuntimeException(
                        "The assigner offers empty finished split information, this should not happen");
            }

            List<FinishedSnapshotSplitInfo> updatedFinishedSnapshotSplitInfos =
                    forwardHighWatermarks(finishedSnapshotSplitInfos, currentBinlogReadingOffset);

            groups = Lists.partition(updatedFinishedSnapshotSplitInfos, groupSize);
            digest = MySqlBinlogSplit.Digest.of(updatedFinishedSnapshotSplitInfos);
        }

        public MySqlBinlogSplit.Digest getDigest() {
            return digest;
        }

        public Optional<List<FinishedSnapshotSplitInfo>> getGroup(int groupId) {
            return (groupId >= 0 && groupId < groups.size())
                    ? Optional.of(groups.get(groupId))
                    : Optional.empty();
        }

        public int getNumberOfGroups() {
            return groups.size();
        }

        /**
         * Forwards the high watermarks of the given {@link FinishedSnapshotSplitInfo}s to the
         * current binlog reading offset.
         *
         * <p>This way, when a source reader start reading the binlog after replicating the new
         * version of the binlog split metadata, it will not re-emit the events that it has already
         * emitted.
         */
        private static List<FinishedSnapshotSplitInfo> forwardHighWatermarks(
                List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
                BinlogOffset currentBinlogReadingOffset) {
            return finishedSnapshotSplitInfos.stream()
                    .map(
                            i -> {
                                if (i.getHighWatermark().isBefore(currentBinlogReadingOffset)) {
                                    // for split has started read binlog, forward its high watermark
                                    // to
                                    // the current binlog reading offset
                                    return new FinishedSnapshotSplitInfo(
                                            i.getTableId(),
                                            i.getSplitId(),
                                            i.getSplitStart(),
                                            i.getSplitEnd(),
                                            currentBinlogReadingOffset);
                                }

                                return i;
                            })
                    .collect(Collectors.toList());
        }
    }
}
