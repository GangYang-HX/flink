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

package org.apache.flink.bili.writer;

import java.util.*;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.bili.writer.metricsetter.MetricSetter;
import org.apache.flink.bili.writer.sink.StreamFileSystemSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.Buckets;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileStatus;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.bili.writer.StreamingFileCommitter.CommitMessage;
import org.apache.flink.trace.Trace;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.bili.writer.FileSystemOptions.*;
import static org.apache.flink.streaming.api.operators.ChainingStrategy.ALWAYS;

/**
 * Operator for file system sink. It is a operator version of {@link StreamingFileSink}.
 * It sends partition commit message to downstream for committing.
 *
 * <p>See {@link StreamingFileCommitter}.
 */
public class StreamingFileWriter<T> extends AbstractStreamOperator<CommitMessage>
        implements OneInputStreamOperator<T, CommitMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamFileSystemSink.class);

    private static final long serialVersionUID = 1L;

    private static final String CREATE_TIME = "ctime";

    private static final String REQUEST_TIME = "request_time";

    private static final String UPDATE_TIME = "update_time";

    // ------------------------ configuration fields --------------------------
    private final Configuration conf;

    private final long bucketCheckInterval;

    private final StreamingFileSink.BucketsBuilder<T, ?, ? extends StreamingFileSink.BucketsBuilder<T, ?, ?>> bucketsBuilder;

    private final InactiveBucketListener listener;

    // --------------------------- runtime fields -----------------------------

    private transient Buckets<T, ?> buckets;

    private transient StreamingFileSinkHelper<T> helper;

    private transient long currentWatermark;

    private transient Set<String> inactivePartitions;

    private Boolean cutoff = true;

    private TreeMap<Long, Boolean> checkpointCutoff = new TreeMap<>();

    private Trace trace;

	private transient Set<String> activePartitions;

    private Object metricSetter;

    /**
     * Used to keep bucketId and bucketPartFile.
     */
    private List<PartFileStatus> bucketIdAndPartFile;
    private ListState<PartFileStatus> bucketIdAndPartFileState;

	private transient ListState<List<String>> createdListState;



    public StreamingFileWriter(
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<T, ?, ? extends StreamingFileSink.BucketsBuilder<T, ?, ?>> bucketsBuilder,
            InactiveBucketListener listener,
            Object metricSetter,
            Configuration conf) {
        this.bucketCheckInterval = bucketCheckInterval;
        this.bucketsBuilder = bucketsBuilder;
        this.listener = listener;
        this.metricSetter = metricSetter;
        this.conf = conf;
        this.chainingStrategy = ALWAYS;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        if (metricSetter instanceof MetricSetter) {
            ((MetricSetter) metricSetter).setMetricGroup(getMetricGroup());
        }
        super.initializeState(context);
		buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getMetricGroup());
        this.trace = Trace.createTraceClient(
                getUserCodeClassloader(),
                conf.getString(SINK_PARTITION_TRACE_KIND),
                conf.getString(SINK_PARTITION_CUSTOM_TRACE_CLASS),
                conf.getString(SINK_PARTITION_TRACE_ID));
        inactivePartitions = new HashSet<>();
		activePartitions = new HashSet<>();
        listener.setInactiveConsumer(b -> inactivePartitions.add(b.toString()));
		listener.setActiveConsumer(b -> activePartitions.add(b.toString()));
		ListStateDescriptor<List<String>> createdPartitionsDescriptor =
			new ListStateDescriptor<>("createdPartitions-state",
				new ListSerializer<>(new StringSerializer()));
		createdListState = context.getOperatorStateStore().getListState(createdPartitionsDescriptor);
		if (context.isRestored()) {
			if (createdListState.get().iterator().hasNext()) {
				this.activePartitions.addAll(createdListState.get().iterator().next());
				LOG.info("created partitions init in writer :{}", this.activePartitions);
			}else {
				LOG.warn("created partitions is null or empty in writer!");
			}
		}
        bucketIdAndPartFile = new ArrayList<>();
        ListStateDescriptor<PartFileStatus> stateDescriptor =
                new ListStateDescriptor<>("PartFileStatus", PartFileStatus.class);
        bucketIdAndPartFileState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()) {
            for (PartFileStatus state : bucketIdAndPartFileState.get()) {
                bucketIdAndPartFile.add(state);
            }
        }

        listener.setPartFileStatusConsumer(b -> {
            synchronized (bucketIdAndPartFile) {
                bucketIdAndPartFile.add((PartFileStatus) b);
            }
        });
		buckets.setBucketLifeCycleListener(listener);
        helper = new StreamingFileSinkHelper<>(
                buckets,
                context.isRestored(),
                context.getOperatorStateStore(),
                getRuntimeContext().getProcessingTimeService(),
                bucketCheckInterval);
        currentWatermark = Long.MIN_VALUE;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        helper.snapshotState(context.getCheckpointId());
        checkpointCutoff.put(context.getCheckpointId(),cutoff);
        cutoff = true;
		createdListState.clear();
		createdListState.add(new ArrayList<>(activePartitions));
        bucketIdAndPartFileState.clear();
        bucketIdAndPartFileState.addAll(bucketIdAndPartFile);

    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        helper.onElement(
                element.getValue(),
                getProcessingTimeService().getCurrentProcessingTime(),
                element.hasTimestamp() ? element.getTimestamp() : null,
                currentWatermark);
        cutoff = false;

        logTrace(element);
    }

    private void logTrace(StreamRecord<T> element){
        Map<String,byte[]> metaMap = new HashMap<>();
        if (conf.getInteger(SINK_PARTITION_META_POS) != -1) {
            Row row = (Row) element.getValue();
            metaMap = (Map<String,byte[]>) row.getField(conf.getInteger(SINK_PARTITION_META_POS));
        }

        Long current = System.currentTimeMillis();
        Long ctime = metaMap.containsKey(CREATE_TIME) ? Long.parseLong(new String(metaMap.get(CREATE_TIME))) : current;
        Long requestTime = metaMap.containsKey(REQUEST_TIME) ? Long.parseLong(new String(metaMap.get(REQUEST_TIME))) : current;
        Long updateTime = metaMap.containsKey(UPDATE_TIME) ? Long.parseLong(new String(metaMap.get(UPDATE_TIME))) : current;

        trace.traceEvent(ctime,1,"batch.output.send","",(int) (current - updateTime),"HDFS");
        trace.traceEvent(requestTime, 1,"output.send", "", (int) (current - requestTime), "HDFS");
    }

    /**
     * Commit up to this checkpoint id, also send inactive partitions to downstream for committing.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        commitUpToCheckpoint(checkpointId);
    }

    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        helper.commitUpToCheckpoint(checkpointId);
		CommitMessage message = new CommitMessage(
			checkpointId,
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks(),
			new ArrayList<>(inactivePartitions),
			new ArrayList<>(activePartitions),
			bucketIdAndPartFile,
			checkpointCutoff.get(checkpointId));
        output.collect(new StreamRecord<>(message));
        inactivePartitions.clear();
		activePartitions.clear();
		bucketIdAndPartFile.clear();
        checkpointCutoff.headMap(checkpointId, true).clear();
    }

    @Override
    public void dispose() throws Exception {
        super.dispose();
        if (helper != null) {
            helper.close();
        }
    }
}
