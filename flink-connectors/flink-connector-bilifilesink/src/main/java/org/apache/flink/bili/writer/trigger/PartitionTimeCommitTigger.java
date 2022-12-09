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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.bili.writer.extractor.PartitionTimeExtractor;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.metricsetter.TriggerMetricsWrapper;
import org.apache.flink.bili.writer.spliter.PathSplitter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

import static org.apache.flink.bili.writer.FileSystemOptions.*;

/**
 * Partition commit trigger by partition time and watermark,
 * if 'watermark' > 'partition-time' + 'delay', will commit the partition.
 *
 * <p>Compares watermark, and watermark is related to records and checkpoint, so we need store
 * watermark information for checkpoint.
 */
public class PartitionTimeCommitTigger extends PartitionCommitTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionTimeCommitTigger.class);

    private static final ListStateDescriptor<Map<Long, Long>> WATERMARKS_STATE_DESC =
        new ListStateDescriptor<>(
            "checkpoint-id-to-watermark",
            new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE));

    private final ListState<Map<Long, Long>> watermarksState;
    private final TreeMap<Long, Long> watermarks;
    private final PartitionTimeExtractor extractor;

    private final PathSplitter splitter;
    private final boolean allowEmptyPartitionCommit;
    private final boolean pathContainPartitionKey;
    private final Long switchTime;
    private final boolean partitionRollback;
    private final String tableFormat;
	private final boolean multiPartitionEmptyCommit;


    private TriggerMetricsWrapper triggerMetricsWrapper;

    public PartitionTimeCommitTigger(
        boolean isRestored,
        OperatorStateStore stateStore,
        Configuration conf,
        ClassLoader cl,
        List<String> partitionKeys,
        TableMetaStoreFactory metaStoreFactory,
        TriggerMetricsWrapper triggerMetricsWrapper) throws Exception {
        super(isRestored, stateStore, conf);
        this.partitionKeys = partitionKeys;
        this.commitDelay = conf.getLong(SINK_PARTITION_COMMIT_DELAY);
        this.extractor = PartitionTimeExtractor.create(
            cl,
            conf.getString(PARTITION_TIME_EXTRACTOR_KIND),
            conf.getString(PARTITION_TIME_EXTRACTOR_CLASS),
            conf.getString(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN));

        this.splitter = PathSplitter.create(
            cl,
            conf.getString(PARTITION_SPLIT_KIND),
            conf.getString(PARTITION_SPLIT_CLASS));

        this.watermarksState = stateStore.getListState(WATERMARKS_STATE_DESC);
        this.watermarks = new TreeMap<>();
        if (isRestored) {
            if (watermarksState.get().iterator().hasNext()) {
                watermarks.putAll(watermarksState.get().iterator().next());
            }
        }
        this.metaStoreFactory = metaStoreFactory;
        this.triggerMetricsWrapper = triggerMetricsWrapper;
        this.allowEmptyPartitionCommit = conf.getBoolean(SINK_PARTITION_ALLOW_EMPTY_COMMIT);
        this.pathContainPartitionKey = conf.getBoolean(SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY);
        this.switchTime = conf.getLong(SINK_PARTITION_EMPTY_SWITCH_TIME);
        this.partitionRollback = conf.getBoolean(SINK_PARTITION_ROLLBACK);
        this.tableFormat = conf.getString(SINK_HIVE_TABLE_FORMAT);
		this.multiPartitionEmptyCommit = conf.getBoolean(SINK_MULTI_PARTITION_EMPTY_COMMIT);

    }

    @Override
    public List<String> committablePartitions(long checkpointId) {
        if (!watermarks.containsKey(checkpointId)) {
            throw new IllegalArgumentException(String.format(
                "Checkpoint(%d) has not been snapshot. The watermark information is: %s.",
                checkpointId, watermarks));
        }
        List<String> needCommit = new ArrayList<>();

		for (String partition : pendingPartitions) {
			if (judgeWaterMark(checkpointId, partition)) {
				needCommit.add(partition);
			}
		}
        return needCommit;
    }

	@Override
	public boolean judgeWaterMark(long checkpointId, String partition) {
		long watermark = watermarks.get(checkpointId);
		boolean cutoff = cutoffMap.get(checkpointId);

		if (!cutoff) {
			this.lastTimestamp = watermark;
		}
		long commitTime = allowEmptyPartitionCommit && cutoff ? System.currentTimeMillis() : watermark;
		commitTime = multiPartitionEmptyCommit && cutoff ? System.currentTimeMillis() : commitTime;
		Long partTime = extractor.extract(
			partitionKeys,  splitter.splitPath(pathContainPartitionKey, partitionKeys, new Path(partition)));
		LOG.info("event time judging time partition {}, commitTime {}, part time {},wk {}",
			partition, new Timestamp(commitTime), new Timestamp(partTime), watermark);
		if (commitTime > partTime + commitDelay) {
			watermarks.headMap(checkpointId,false).clear();
			cutoffMap.headMap(checkpointId, false).clear();
			return true;
		}
		return false;

	}

	@Override
    public void snapshotState(long checkpointId, long watermark) throws Exception {
        LOG.info("checkpointId : {}, watermark : {}", checkpointId, watermark);
        long wm = watermark == Long.MIN_VALUE&&watermarks.containsKey(checkpointId-1)? watermarks.get(checkpointId-1) : watermark;
        super.snapshotState(checkpointId, wm);
        watermarks.put(checkpointId, wm);

        watermarksState.clear();
        watermarksState.add(new HashMap<>(watermarks));
    }

    /**
     *
     * @param checkpointId
     * @param cutoff
     */
    @Override
    public void mergeCutoff(Long checkpointId, boolean cutoff) {
        LOG.info("checkpointId is {}, cutoff is {}", checkpointId, cutoff);
        cutoffMap.compute(checkpointId, (k, v) -> null == v ? cutoff : v&&cutoff);

    }

    @Override
    public void checkAndAddEmptyPartitions(long checkpointId) throws Exception {
        //allow create empty partition when stream is cutoff
        if (!cutoffMap.containsKey(checkpointId)) {
            return;
        }

        long now = System.currentTimeMillis();
        //切表延迟20分钟校验，防止提前创建空分区
        long delaySwitchTime = switchTime + 1200_000L;
        if (switchTime != 0L ) {
            //临界点时间lzo->orc表，防止另外一个任务提交空分区信息
            if(!partitionRollback && ((tableFormat.equals("text") && now >= switchTime) || (tableFormat.equals("orc") && now < delaySwitchTime))) {
                return;
            }
            //回滚orc->text表，防止另外一个任务提交空分区信息
            if(partitionRollback && ((tableFormat.equals("orc") && now >= switchTime) || (tableFormat.equals("text") && now < delaySwitchTime))) {
                return;
            }
        }

        Boolean cutoff = cutoffMap.get(checkpointId);
        LOG.info("checkAndAddEmptyPartitions,checkpointId : {},cutoff : {}", checkpointId, cutoff);

        if (allowEmptyPartitionCommit && cutoff) {
            super.checkAndAddEmptyPartitions(checkpointId);
        }
    }
}
