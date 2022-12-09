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

package org.apache.flink.bili.writer.trigger;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.metricsetter.TriggerMetricsWrapper;
import org.apache.flink.bili.writer.util.PartPathUtils;
import org.apache.flink.bili.writer.util.PartTimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.bili.writer.FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER;
import static org.apache.flink.bili.writer.FileSystemOptions.SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY;

//import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER;

/**
 * Abstract commit trigger, store partitions in state and provide {@link #committablePartitions}
 * for trigger.
 * See {@link PartitionTimeCommitTigger}.
 * See {@link ProcTimeCommitTigger}.
 */
public abstract class PartitionCommitTrigger {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionCommitTrigger.class);

    public static final String PARTITION_TIME = "partition-time";
    public static final String PROCESS_TIME = "process-time";
    public static final String PROCESS_PART_TIME = "process-part-time";
    protected ScheduledExecutorService scheduledExecutorService;

    private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
        new ListStateDescriptor<>(
            "pending-partitions",
            new ListSerializer<>(StringSerializer.INSTANCE));
    private static final ListStateDescriptor LAST_TIMESTAMP_STATE_DESC =
        new ListStateDescriptor("last-timestamp", LongSerializer.INSTANCE);
    private final ListState<Long> lastTimestampState;
    private final boolean pathContainPartitionKey;

    protected final ListState<List<String>> pendingPartitionsState;
    protected final Set<String> pendingPartitions;
    protected long lastTimestamp;
    protected long commitDelay;
    protected List<String> partitionKeys;
    protected TableMetaStoreFactory metaStoreFactory;

    protected  TreeMap<Long, Boolean> cutoffMap = new TreeMap<>();

	protected final ListState<Map<String, Long>> openCountState;
	protected final ListState<Map<String, Long>> closeCountState;
	public final Map<String,Long> partitionOpenCount;
	public final Map<String,Long> partitionCloseCount;
	private static final ListStateDescriptor<Map<String, Long>> OPEN_COUNT_STATE_DESC =
		new ListStateDescriptor<>(
			"open-count",
			new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE));
	private static final ListStateDescriptor<Map<String, Long>> CLOSE_COUNT_STATE_DESC =
		new ListStateDescriptor<>(
			"close-count",
			new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE));




    protected PartitionCommitTrigger(
        boolean isRestored, OperatorStateStore stateStore, Configuration conf) throws Exception {
        this.pendingPartitionsState = stateStore.getListState(PENDING_PARTITIONS_STATE_DESC);
		this.openCountState = stateStore.getListState(OPEN_COUNT_STATE_DESC);
		this.closeCountState = stateStore.getListState(CLOSE_COUNT_STATE_DESC);
        this.pendingPartitions = new HashSet<>();
		this.partitionOpenCount = new HashMap<>();
		this.partitionCloseCount = new HashMap<>();
        if (isRestored) {
            pendingPartitions.addAll(pendingPartitionsState.get().iterator().next());

			for (Map<String, Long> map : openCountState.get()){
				partitionOpenCount.putAll(map);
			}
			for (Map<String, Long> map : closeCountState.get()){
				partitionCloseCount.putAll(map);
			}
        }
        this.lastTimestampState = stateStore.getListState(LAST_TIMESTAMP_STATE_DESC);
        if (lastTimestampState != null && lastTimestampState.get().iterator().hasNext()) {
            lastTimestamp = lastTimestampState.get().iterator().next();
        }
        this.pathContainPartitionKey = conf.getBoolean(SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY);



    }


    /**
     * protect thread safe
     * Add a pending partition.
     */
    public void addPartition(String partition) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.add(partition);
        }
    }

    /**
     * Get committable partitions, and cleanup useless watermarks and partitions.
     */
    public abstract List<String> committablePartitions(long checkpointId) throws IOException;

    /**
     * End input, return committable partitions and clear.
     */
    public List<String> endInput() {
        ArrayList<String> partitions = new ArrayList<>(pendingPartitions);
        pendingPartitions.clear();
        return partitions;
    }

    /**
     *
     * @param checkpointId
     * @param cutoff
     */
    public void mergeCutoff(Long checkpointId, boolean cutoff) {
        //override by subclass
    }

    /**
     * Snapshot state.
     */
    public void snapshotState(long checkpointId, long watermark) throws Exception {
        pendingPartitionsState.clear();
        pendingPartitionsState.add(new ArrayList<>(pendingPartitions));
        LOG.info("snapshot timestamp: {}", lastTimestamp);
        lastTimestampState.clear();
        lastTimestampState.add(lastTimestamp);
		openCountState.clear();
		openCountState.add(new HashMap<>(partitionOpenCount));
		closeCountState.clear();
		closeCountState.add(new HashMap<>(partitionCloseCount));

		Iterator<String> iterator = pendingPartitions.iterator();
		while (iterator.hasNext()){
			String partition = iterator.next();
			LOG.info("partition = {},openCount = {},closeCount = {}",partition,partitionOpenCount.get(partition),partitionCloseCount.get(partition));
		}
    }

    public static PartitionCommitTrigger create(
        boolean isRestored,
        OperatorStateStore stateStore,
        Configuration conf,
        ClassLoader cl,
        List<String> partitionKeys,
        ProcessingTimeService procTimeService,
        FileSystem fileSystem,
        Path locationPath,
        TableMetaStoreFactory metaStoreFactory,
        TriggerMetricsWrapper triggerMetricsWrapper) throws Exception {


        //这里会选择根据哪个时间去commit
        String trigger = conf.getString(SINK_PARTITION_COMMIT_TRIGGER);
        switch (trigger) {
            case PARTITION_TIME:
                return new PartitionTimeCommitTigger(
                    isRestored, stateStore, conf, cl, partitionKeys, metaStoreFactory, triggerMetricsWrapper);
            case PROCESS_TIME:
                return new ProcTimeCommitTigger(
                    isRestored, stateStore, conf, procTimeService, fileSystem, locationPath, metaStoreFactory);
            case PROCESS_PART_TIME:
                return new ProcTimePartitionCommitTigger(
                    isRestored, stateStore, conf, cl, procTimeService, partitionKeys, metaStoreFactory);
            default:
                throw new UnsupportedOperationException(
                    "Unsupported partition commit trigger: " + trigger);
        }
    }

    /**
     * 空分区创建
     */
    public void checkAndAddEmptyPartitions(long checkpointId) throws Exception {
        long watermark = System.currentTimeMillis();
        if (lastTimestamp == 0L || lastTimestamp == Long.MIN_VALUE) {
            lastTimestamp = PartTimeUtils.getLastHourTimestamp(watermark);
            LOG.info("init lastTimestamp = {},watermark:{}", lastTimestamp, watermark);
        }
        boolean flag = PartPathUtils.isHourPath(partitionKeys);
        List<Long> needAddTimestamps = flag ? PartTimeUtils.getTimestampGap(lastTimestamp, watermark, commitDelay)
                : PartTimeUtils.getDayTimestampGap(lastTimestamp, watermark, commitDelay);
        if (null == needAddTimestamps) {
            LOG.info("do not need add empty partition,lastTimestamp:{} watermark:{},commitDelay:{}.", lastTimestamp, watermark, commitDelay);
            return;
        }
        LOG.info("need to add empty partitions,watermark : {},lastTimestamp :{},commitDelay:{}", watermark, lastTimestamp, commitDelay);
        for (long timestamp : needAddTimestamps) {
            String pathWithDate = PartPathUtils.concatPartitionPath(pathContainPartitionKey, timestamp, partitionKeys);
            LOG.info("metastore need add timestamp :{},pathWithDate:{}", needAddTimestamps, pathWithDate);
            addPartition(pathWithDate);
        }
        this.lastTimestamp = watermark;
    }
	public boolean judgeWaterMark(long checkpointId,String partition){
		return false;
	}

    public Set<String> pendingPartitions() {
        return pendingPartitions;
    }

	public boolean shouldClosed(long checkpoint, String partition) {

		long closeCount = partitionCloseCount.get(partition) == null ? 0L : partitionCloseCount.get(partition);
		long openCount = partitionOpenCount.get(partition) == null ? 0L : partitionOpenCount.get(partition);
		if (closeCount > openCount) {
			LOG.error("closeCount > openCount : closeCount = {},openCount = {},partition = {}"
				, closeCount, openCount, partition);
		}
		boolean flag = judgeWaterMark(checkpoint, partition);
		if (flag && closeCount < openCount) {
			LOG.warn("watermark is right but open not equals close count, closeCount:{}, openCount:{}, partition:{}," +
					" checkpoint :{}.",
				closeCount, openCount, partition, checkpoint);
		}
		return closeCount >= openCount;
	}

	public Map<String,Long> addCreatedPartitions(List<String> createdPartitions,int taskId){
		pendingPartitions.addAll(createdPartitions);
		for (String createdPartition : createdPartitions){
			long count = partitionOpenCount.get(createdPartition) == null ? 0L : partitionOpenCount.get(createdPartition);
			count = count + 1;
			partitionOpenCount.put(createdPartition,count);
			LOG.info("partition created increase: {}，taskId = {},count = {}",createdPartition,taskId,count);
		}
		return partitionOpenCount;
	}

	public void addClosePartitions(List<String> partitions,int taskId) {
		for (String partition : partitions){
			long count = partitionCloseCount.get(partition) == null ? 0L : partitionCloseCount.get(partition);
			count = count + 1;
			partitionCloseCount.put(partition,count);
			LOG.info("partition closed increase: {}，taskId = {},count = {}",partition,taskId,count);
		}
	}

	public void closePartition(String partition){
		partitionOpenCount.remove(partition);
		partitionCloseCount.remove(partition);
	}

}
