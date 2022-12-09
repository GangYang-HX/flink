/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive.bili;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.table.ContinuousPartitionFetcher;
import org.apache.flink.connectors.hive.ContinuousHivePendingSplitsCheckpoint;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveSourceFileEnumerator;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionContext;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** A continuously monitoring {@link SplitEnumerator} for hive source. */
public class ContinuousHiveSplitEnumeratorNew<T extends Comparable<T>>
        implements SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousHiveSplitEnumeratorNew.class);

    private final SplitEnumeratorContext<HiveSourceSplit> enumeratorContext;
    private final LinkedHashMap<Integer, String> readersAwaitingSplit;
    private final FileSplitAssigner splitAssigner;
    private final long discoveryInterval;
    private static volatile String sendSplitId = "";

    private final HiveTableSource.HiveContinuousPartitionFetcherContext<T> fetcherContext;

    // the maximum partition read offset seen so far
    private T currentReadOffset;
    // the partitions that have been processed for the current read offset
    private Collection<List<String>> seenPartitionsSinceOffset;
    private Collection<Path> pathsAlreadyProcessed;
    private final PartitionMonitor<T> monitor;

    public ContinuousHiveSplitEnumeratorNew(
            SplitEnumeratorContext<HiveSourceSplit> enumeratorContext,
            T currentReadOffset,
            Collection<List<String>> seenPartitionsSinceOffset,
            FileSplitAssigner splitAssigner,
            long discoveryInterval,
            int threadNum,
            JobConf jobConf,
            ObjectPath tablePath,
            ContinuousPartitionFetcher<Partition, T> fetcher,
            HiveTableSource.HiveContinuousPartitionFetcherContext<T> fetcherContext,
            Collection<Path> alreadyProcessedPaths) {
        this.enumeratorContext = enumeratorContext;
        this.currentReadOffset = currentReadOffset;
        this.seenPartitionsSinceOffset = new ArrayList<>(seenPartitionsSinceOffset);
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.fetcherContext = fetcherContext;
        readersAwaitingSplit = new LinkedHashMap<>();
        this.pathsAlreadyProcessed = alreadyProcessedPaths;
        monitor =
                new PartitionMonitor<>(
                        currentReadOffset,
                        seenPartitionsSinceOffset,
                        tablePath,
                        threadNum,
                        jobConf,
                        fetcher,
                        fetcherContext, alreadyProcessedPaths);
    }

    @Override
    public void start() {
        try {
            fetcherContext.open();
            LOG.info("Hive Meta Store Client Open Successfully");
            enumeratorContext.callAsync(
                    monitor, this::handleNewSplits, discoveryInterval, discoveryInterval);
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to start continuous split enumerator", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostName) {
        readersAwaitingSplit.put(subtaskId, hostName);
        assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<HiveSourceSplit> splits, int subtaskId) {
        LOG.debug("Continuous Hive Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(new ArrayList<>(splits));
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint<HiveSourceSplit> snapshotState(long checkpointId)
            throws Exception {
        Collection<HiveSourceSplit> remainingSplits =
                (Collection<HiveSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();
        LOG.info(
                "snapshotState pathsAlreadyProcessed detail,{}",
                pathsAlreadyProcessed
                        .stream()
                        .map(item -> item.getPath())
                        .collect(Collectors.toSet()));
        return new ContinuousHivePendingSplitsCheckpoint(
                remainingSplits,
                currentReadOffset,
                seenPartitionsSinceOffset,
                pathsAlreadyProcessed);
    }

    @Override
    public void close() throws IOException {
        try {
            fetcherContext.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void handleNewSplits(NewSplitsAndState<T> newSplitsAndState, Throwable error) {
        if (error != null) {
            // we need to failover because the worker thread is stateful
            throw new FlinkHiveException("Failed to enumerate files", error);
        }
        LOG.info(
                "start new assign hive split,new split size = {}",
                newSplitsAndState.newSplits.size());
        this.currentReadOffset = newSplitsAndState.offset;
        this.seenPartitionsSinceOffset = newSplitsAndState.seenPartitions;
        this.pathsAlreadyProcessed = newSplitsAndState.pathsAlreadyProcessed;
        splitAssigner.addSplits(new ArrayList<>(newSplitsAndState.newSplits)); //TODO 是否先倒排，保证getNext()的时候获取的split是最早的，保证分区消费从最早的开始消费
        assignSplits();
        LOG.info(
                "new split has assign finished,new split size = {},{}",
                newSplitsAndState.newSplits.size());
    }

    private void assignSplits() {
        final Iterator<Map.Entry<Integer, String>> awaitingReader =
                readersAwaitingSplit.entrySet().iterator();
        while (awaitingReader.hasNext()) {
            // There is a certain possibility: the newly discovered split and the old split are
            // sent to the currently requested subtask
            final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
            final String hostname = nextAwaiting.getValue();
            final int awaitingSubtask = nextAwaiting.getKey();
            final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname); //获取集合中最后一个split，如果集合中的元素是从早到最新放置，那么此时会获取最新的split
            if (nextSplit.isPresent()) {
                HiveSourceSplit hiveSourceSplit = (HiveSourceSplit) nextSplit.get();
                LOG.info(
                        "send split to reader,splitId={},hasSend={},path={}",
                        hiveSourceSplit.splitId(),
                        hiveSourceSplit.splitId().equals(sendSplitId),
                        hiveSourceSplit.path().getPath());
                enumeratorContext.assignSplit(hiveSourceSplit, awaitingSubtask);
                sendSplitId = hiveSourceSplit.splitId();
                awaitingReader.remove();
            } else {
                break;
            }
        }
    }

    static class PartitionMonitor<T extends Comparable<T>>
            implements Callable<NewSplitsAndState<T>> {

        // keep these locally so that we don't need to share state with main thread
        private T currentReadOffset;
        private final Set<List<String>> seenPartitionsSinceOffset;

        private final ObjectPath tablePath;
        private final int threadNum;
        private final JobConf jobConf;
        private final ContinuousPartitionFetcher<Partition, T> fetcher;
        private final HiveContinuousPartitionContext<Partition, T> fetcherContext;
        private final HashSet<Path> pathsAlreadyProcessed;

        PartitionMonitor(
                T currentReadOffset,
                Collection<List<String>> seenPartitionsSinceOffset,
                ObjectPath tablePath,
                int threadNum,
                JobConf jobConf,
                ContinuousPartitionFetcher<Partition, T> fetcher,
                HiveContinuousPartitionContext<Partition, T> fetcherContext,
                Collection<Path> pathsAlreadyProcessed) {
            this.currentReadOffset = currentReadOffset;
            this.seenPartitionsSinceOffset = new HashSet<>(seenPartitionsSinceOffset);
            this.tablePath = tablePath;
            this.threadNum = threadNum;
            this.jobConf = jobConf;
            this.fetcher = fetcher;
            this.fetcherContext = fetcherContext;
            this.pathsAlreadyProcessed = new HashSet<>(pathsAlreadyProcessed);
        }

        @Override
        public NewSplitsAndState<T> call() throws Exception {
            LOG.info("Start getting partition information,currentReadOffset={}", currentReadOffset);
            List<Tuple2<Partition, T>> partitions =
                    fetcher.fetchPartitions(fetcherContext, currentReadOffset);
            if (partitions.isEmpty()) {
                return new NewSplitsAndState<>(
                        Collections.emptyList(),
                        currentReadOffset,
                        seenPartitionsSinceOffset,
                        pathsAlreadyProcessed);
            }
            List<Tuple2<Partition, T>> realPartitions = partitions
                    .stream()
                    .filter(item -> !item.f0.getParameters().containsKey("hive_virtual_partition"))
                    .sorted(Comparator.comparing(o -> o.f1))
                    .collect(Collectors.toList());
            List<HiveSourceSplit> newSplits = new ArrayList<>();
            // the max offset of new partitions
            T maxOffset = currentReadOffset;
            Set<List<String>> nextSeen = new HashSet<>();
            for (Tuple2<Partition, T> tuple2 : realPartitions) {
                Partition partition = tuple2.f0;
                List<String> partSpec = partition.getValues();
                if (seenPartitionsSinceOffset.add(partSpec)) {
                    T offset = tuple2.f1;
                    if (offset.compareTo(currentReadOffset) >= 0) {
                        nextSeen.add(partSpec);
                    }
                    if (offset.compareTo(maxOffset) >= 0) {
                        maxOffset = offset;
                    }
                    LOG.info(
                            "Found new partition {} of table {}, generating splits for it",
                            partSpec,
                            tablePath.getFullName());
                    List<HiveSourceSplit> hiveSourceSplits =
                            HiveSourceFileEnumerator.createInputSplits(
                                    0,
                                    Collections.singletonList(
                                            fetcherContext.toHiveTablePartition(partition)),
                                    pathsAlreadyProcessed,
                                    threadNum,
                                    jobConf);
                    newSplits.addAll(hiveSourceSplits);
                    deleteProcessedPartitionFile(pathsAlreadyProcessed, partition);

                }
            }
            currentReadOffset = maxOffset;
            if (!nextSeen.isEmpty()) { //表示发现了一个新的分区，之前的分区信息可以删除
                seenPartitionsSinceOffset.clear();
                seenPartitionsSinceOffset.addAll(nextSeen);
            }

            //虚拟分区数据处理
            handlerVirPartition(partitions
                    .stream()
                    .filter(item -> item.f0.getParameters().containsKey("hive_virtual_partition"))
                    .sorted(Comparator.comparing(o -> o.f1))
                    .collect(Collectors.toList()), newSplits);
            return new NewSplitsAndState<>(
                    newSplits,
                    currentReadOffset,
                    seenPartitionsSinceOffset,
                    pathsAlreadyProcessed);
        }

        /**
         * 因为tuple2.f1 是手动生成的，不是严格的，避免影响currentReadOffset推进，虚拟分区单独处理,同时考虑虚拟分区是按文件级别进行读取
         * <p>
         * 要注意最新未被创建的分区被处理后，后续分区真正创建之后的数据不能再次被处理
         * 因为seenPartitionsSinceOffset被设置了虚拟分区的信息，等这个分区真正创建的时候因为已经作为虚拟分区处理过了，然后就不会根据这个分区的信息推进currentReadOffset
         *
         * @param newSplits
         *
         * @throws Exception
         */
        private void handlerVirPartition(
                List<Tuple2<Partition, T>> virPartitions,
                List<HiveSourceSplit> newSplits) throws Exception {
            for (Tuple2<Partition, T> tupPartition : virPartitions) {
                Partition virPartition = tupPartition.f0;
                LOG.info(
                        "Start generate split for virtual partition,{}",
                        virPartition.getSd().getLocation());
                List<HiveSourceSplit> hiveSourceSplits =
                        HiveSourceFileEnumerator.createInputSplits(
                                0,
                                Collections.singletonList(
                                        fetcherContext.toHiveTablePartition(virPartition)),
                                pathsAlreadyProcessed,
                                threadNum,
                                jobConf);
                LOG.info(
                        "The virtual partition split is successfully generated, split size: {},split details:{}",
                        hiveSourceSplits.size(),
                        hiveSourceSplits.stream()
                                .map(item -> item.path().getPath())
                                .collect(Collectors.toList()));
                newSplits.addAll(hiveSourceSplits);
            }
        }

        /**
         * 这个行为只会发生在分区创建完成之后，因为分区创建之后就算添加新的文件也不会读取，所以这个分区对应的文件后续也不需要再过滤，因此删除掉这部分文件
         * 删除已处理分区的文件信息，避免状态过大以及文件过滤时间过长等问题
         *
         * @param pathsAlreadyProcessed
         * @param partition
         */
        private void deleteProcessedPartitionFile(
                HashSet<Path> pathsAlreadyProcessed,
                Partition partition) {
            LOG.info(
                    "Start deleting files that have created partitions in hive-meta,for {}",
                    partition.getValues());
            final List<String> partValues = partition.getValues();
            final boolean dayPart = partValues.size() == 1;
            Set<Path> partitionPath = pathsAlreadyProcessed.stream().filter(new Predicate<Path>() {
                @Override
                public boolean test(Path path) {
                    if (dayPart) {
                        return path
                                .getPath()
                                .contains(String.format("log_date=%s", partValues.get(0)));
                    }
                    return path
                            .getPath()
                            .contains(String.format(
                                    "log_date=%s/log_hour=%s",
                                    partValues.get(0),
                                    partValues.get(1)));
                }
            }).collect(Collectors.toSet());
            LOG.info(
                    "Delete the number of files that have created partitions in hive-meta，the delete file size={}",
                    partitionPath.size());
            pathsAlreadyProcessed.removeAll(partitionPath);
        }
    }

    /** The result passed from monitor thread to main thread. */
    static class NewSplitsAndState<T extends Comparable<T>> {
        private final T offset;
        private final Collection<List<String>> seenPartitions;
        private final Collection<HiveSourceSplit> newSplits;
        private final Collection<Path> pathsAlreadyProcessed;

        private NewSplitsAndState(
                Collection<HiveSourceSplit> newSplits,
                T offset,
                Collection<List<String>> seenPartitions,
                Collection<Path> pathsAlreadyProcessed) {
            this.newSplits = newSplits;
            this.offset = offset;
            this.seenPartitions = new ArrayList<>(seenPartitions);
            this.pathsAlreadyProcessed = pathsAlreadyProcessed;
        }

        @VisibleForTesting
        Collection<List<String>> getSeenPartitions() {
            return seenPartitions;
        }
    }
}
