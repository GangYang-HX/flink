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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.bili.writer.commitpolicy.MetastoreCommitPolicy;
import org.apache.flink.bili.writer.commitpolicy.PartitionCommitPolicy;
import org.apache.flink.bili.writer.exception.PartitionNotReadyException;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.metricsetter.TriggerMetricsWrapper;
import org.apache.flink.bili.writer.trigger.IndexFileCommitTrigger;
import org.apache.flink.bili.writer.trigger.PartitionCommitTrigger;
import org.apache.flink.bili.writer.util.PartPathUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileStatus;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.trace.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.bili.writer.FileSystemOptions.*;
import static org.apache.flink.bili.writer.partition.PartitionPathUtils.extractPartitionSpecFromPath;
import static org.apache.flink.bili.writer.partition.PartitionPathUtils.extractPartitionSpecFromPathWithoutPartitionKey;


/**
 * Committer for {@link StreamingFileWriter}. This is the single (non-parallel) task.
 * It collects all the partition information sent from upstream, and triggers the partition
 * submission decision when it judges to collect the partitions from all tasks of a checkpoint.
 *
 * <p>Processing steps:
 * 1.Partitions are sent from upstream. Add partition to trigger.
 * 2.{@link TaskTracker} say it have already received partition data from all tasks in a checkpoint.
 * 3.Extracting committable partitions from {@link PartitionCommitTrigger}.
 * 4.Using {@link PartitionCommitPolicy} chain to commit partitions.
 *
 * <p>See {@link StreamingFileWriter#notifyCheckpointComplete}.
 */
public class StreamingFileCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<StreamingFileCommitter.CommitMessage, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingFileCommitter.class);

    private static final long serialVersionUID = 1L;

    private final Configuration conf;

    private final Path locationPath;

    private final ObjectIdentifier tableIdentifier;

    private final List<String> partitionKeys;

    //这个metaStoreFactory就是在FileSystemTableSink里面的createTableMetaStoreFactory
    private final TableMetaStoreFactory metaStoreFactory;

    private transient PartitionCommitTrigger trigger;

    private transient TaskTracker taskTracker;

    private transient long currentWatermark;

    private TableMetaStoreFactory.TableMetaStore metaStore;

    private transient List<PartitionCommitPolicy> policies;

    private TriggerMetricsWrapper triggerMetricsWrapper;

    private static final String FAIL_BACK_WATERMARK = "failBackWatermark";

    private final boolean eagerCommit;

    private String system_user_id;

    private Trace trace;

    private final boolean pathContainPartitionKey;

	private final Set<String> commitIdSet;

    /**
     * 是否允许生成索引文件
     */
    private boolean enableIndexFile;

    private IndexFileCommitTrigger indexFileTrigger;
	private final int metastoreLimit;
	private final boolean defaultPartitionCheck;



    public StreamingFileCommitter(
        Path locationPath,
        ObjectIdentifier tableIdentifier,
        List<String> partitionKeys,
        TableMetaStoreFactory metaStoreFactory,
        Configuration conf) {
        this.locationPath = locationPath;
        this.tableIdentifier = tableIdentifier;
        this.partitionKeys = partitionKeys;
        this.metaStoreFactory = metaStoreFactory;
        this.conf = conf;
        this.metaStore = null;
        this.eagerCommit = conf.getBoolean(SINK_PARTITION_COMMIT_EAGERLY);
        this.pathContainPartitionKey = conf.getBoolean(SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY);
        LOG.info("commit policy: {}", eagerCommit);
        commitIdSet = new HashSet<>();
        this.enableIndexFile = conf.getBoolean(SINK_PARTITION_ENABLE_INDEX_FILE);

		this.metastoreLimit = conf.getInteger(SINK_METASTORE_COMMIT_LIMIT_NUM);
		LOG.info("metastore commit limit :{}", metastoreLimit);
		this.defaultPartitionCheck = conf.getBoolean(SINK_DEFAULT_PARTITION_CHECK);
		LOG.info("default partition check :{}", defaultPartitionCheck);
	}

    @Override
    public void open() throws Exception {
        super.open();
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("BiliFileSink");
        this.triggerMetricsWrapper = new TriggerMetricsWrapper().setFailBackWatermark(metricGroup.counter(FAIL_BACK_WATERMARK));
        this.trace = Trace.createTraceClient(
                getUserCodeClassloader(),
                conf.getString(SINK_PARTITION_TRACE_KIND),
                conf.getString(SINK_PARTITION_CUSTOM_TRACE_CLASS),
                conf.getString(SINK_PARTITION_TRACE_ID));
        LOG.info("trigger wrapper is initial.");
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
        this.system_user_id = getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration().getString(SYSTEM_USER_ID);
        LOG.info("system user id:{}", system_user_id);
        LOG.info("setup conf1:{}", config);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        FileSystem fileSystem = locationPath.getFileSystem();
        currentWatermark = Long.MIN_VALUE;
        this.trigger = PartitionCommitTrigger.create(
            context.isRestored(),
            context.getOperatorStateStore(),
            conf,
            getUserCodeClassloader(),
            partitionKeys,
            getProcessingTimeService(),
            fileSystem,
            locationPath,
            metaStoreFactory,
            triggerMetricsWrapper);
        this.policies = PartitionCommitPolicy.createPolicyChain(
            getUserCodeClassloader(),
            conf.getString(SINK_PARTITION_COMMIT_POLICY_KIND),
            conf.getString(SINK_PARTITION_COMMIT_POLICY_CLASS),
            conf.getString(SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME),
            fileSystem);
        trigger.checkAndAddEmptyPartitions(Long.MIN_VALUE);
        if (enableIndexFile) {
			indexFileTrigger = new IndexFileCommitTrigger(
				context.isRestored(),
				context.getOperatorStateStore(),
				conf,
				fileSystem);
			indexFileTrigger.init();
		}


	}

    @Override
    public void processElement(StreamRecord<CommitMessage> element) throws Exception {
        CommitMessage message = element.getValue();
        for (String partition : message.partitions) {
            trigger.addPartition(partition);
        }
        commitIdSet.addAll(message.createdPartitions);
		trigger.addCreatedPartitions(message.createdPartitions, message.taskId);
		trigger.addClosePartitions(message.partitions, message.taskId);
        if (enableIndexFile) {
            indexFileTrigger.append(message.getBucketIdAndPartFile());
        }

        trigger.mergeCutoff(message.checkpointId, message.cutoff);
        if (taskTracker == null) {
            taskTracker = new TaskTracker(message.numberOfTasks);
        }
        boolean needCommit = taskTracker.add(message.checkpointId, message.taskId);
        if (needCommit) {
            //空分区检测
            trigger.checkAndAddEmptyPartitions(message.checkpointId);
            commitPartitions(message.checkpointId);
            if (enableIndexFile) {
                indexFileTrigger.notifyCheckpointComplete();
            }
        }
    }

    private void commitPartitions(long checkpointId) throws Exception {
        List<String> partitions = checkpointId == Long.MAX_VALUE ?
                trigger.endInput() : trigger.committablePartitions(checkpointId);
        if (partitions.isEmpty()) {
            return;
        }
		try {
			this.metaStore = metaStoreFactory.getTableMetaStore(this.metaStore);
			for (int i = 0; i < partitions.size(); i++) {
				Path parentPath = metaStore.getLocationPath();
				LOG.info("commit a partition in parent dir: {}, partition name: {}", parentPath, partitions.get(i));
				Path path = new Path(metaStore.getLocationPath(), partitions.get(i));
				LOG.info("metastore location path:{}", path.getPath());
				LOG.info("policy number:{}", policies.size());
				LOG.info("normal commit");
				for (PartitionCommitPolicy policy : policies) {
					PartitionCommitPolicy.Context context;
					context = new PolicyContext(
						new ArrayList<>(), path, trigger.partitionOpenCount.keySet(),
						trigger.partitionCloseCount.keySet(), checkpointId, false);
					if (policy instanceof MetastoreCommitPolicy) {
						if (((MetastoreCommitPolicy) policy).checkPartitionSize(i, metastoreLimit)) {
							((MetastoreCommitPolicy) policy).setMetastore(metaStore);
							((MetastoreCommitPolicy) policy).setTriggerMetricsWrapper(triggerMetricsWrapper);
							((MetastoreCommitPolicy) policy).setTrace(trace);
						}
					}
					commitWithMultiPartitionCheck(context, policy, checkpointId, partitions.get(i));
				}
				trigger.pendingPartitions().remove(partitions.get(i));
				trigger.closePartition(partitions.get(i));
			}
		} catch (PartitionNotReadyException e) {
			LOG.info("partition not ready for all partitions, open partitions:{}, committed partitions :{}, errorMsg:{}  ",
				trigger.partitionOpenCount.keySet(), trigger.partitionCloseCount.keySet(), e.getMessage());
		} catch (Exception e) {
			LOG.error("commit policy error", e);
		}

    }
	private void commitWithMultiPartitionCheck(PartitionCommitPolicy.Context context, PartitionCommitPolicy policy,
											   long checkpointId, String partition) throws Exception {
		if (defaultPartitionCheck) {
			if (trigger.shouldClosed(checkpointId, partition)) {
				policy.commit(context);
			} else {
				throw new PartitionNotReadyException("partition " + partition + " not ready, closed != open");
			}
		} else {
			policy.commit(context);
		}
	}

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        this.currentWatermark = mark.getTimestamp();
    }

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		trigger.snapshotState(context.getCheckpointId(), currentWatermark);
		LOG.info("snapshot ing ... commitId Set's size is {} ,eager commit:eagerCommit flag:{}",commitIdSet.size(),eagerCommit);
		if (eagerCommit) {
			executeEagerlyCommit();
		}
		commitIdSet.clear();
        if (enableIndexFile) {
            indexFileTrigger.snapshotState();
        }
	}

    @Override
    public void close() throws Exception {
        super.close();
        if (enableIndexFile) {
            indexFileTrigger.close();
        }
    }

	private void executeEagerlyCommit() throws Exception {
		this.metaStore = metaStoreFactory.getTableMetaStore(this.metaStore);
		MetastoreCommitPolicy metaCommit = new MetastoreCommitPolicy();
		metaCommit.setTrace(trace);
		metaCommit.setMetastore(metaStore);
		metaCommit.setTriggerMetricsWrapper(triggerMetricsWrapper);
		commitIdSet.forEach(e -> {
			PartitionCommitPolicy.Context pathContext = new PolicyContext(new ArrayList<>(),
				new Path(metaStore.getLocationPath(), e), eagerCommit);//加上标识符标志是eagerCommit
			LOG.info("eagerly commit partition :{}, path:{}", e, metaStore.getLocationPath());
			try {
				metaCommit.commit(pathContext);
			} catch (Exception exception) {
				LOG.error("snapshot commit error: {}, will retry again at next snapshot!",
					new Path(metaStore.getLocationPath(), e), exception);
			}
		});
	}

    /**
     * The message sent upstream.
     *
     * <p>Need to ensure that the partitions are ready to commit. That is to say, the files in
     * the partition have become readable rather than temporary.
     */
    public static class CommitMessage implements Serializable {

        public long checkpointId;
        public int taskId;
        public int numberOfTasks;
        public List<String> partitions;
        public List<String> createdPartitions;

        /**
         * Used to keep bucketId and part file.
         */
        List<PartFileStatus> bucketIdAndPartFile;

        public boolean cutoff;
        /**
         * Pojo need this constructor.
         */
        public CommitMessage() {
        }

        public CommitMessage(
            long checkpointId, int taskId, int numberOfTasks, List<String> partitions,
			List<String> createdPartitions,List<PartFileStatus> bucketIdAndPartFile, boolean cutoff) {
            this.checkpointId = checkpointId;
            this.taskId = taskId;
            this.numberOfTasks = numberOfTasks;
            this.partitions = partitions;
            this.cutoff = cutoff;
			this.createdPartitions = createdPartitions;
            this.bucketIdAndPartFile = bucketIdAndPartFile;
        }

        public List<PartFileStatus> getBucketIdAndPartFile() {
            return bucketIdAndPartFile;
        }
    }

    /**
     * Track the upstream tasks to determine whether all the upstream data of a checkpoint
     * has been received.
     */
    private static class TaskTracker {

        private final int numberOfTasks;

        /**
         * Checkpoint id to notified tasks.
         */
        private TreeMap<Long, Set<Integer>> notifiedTasks = new TreeMap<>();

        private TaskTracker(int numberOfTasks) {
            this.numberOfTasks = numberOfTasks;
        }

        /**
         * @return true, if this checkpoint id need be committed.
         */
        private boolean add(long checkpointId, int task) {
            Set<Integer> tasks = notifiedTasks.computeIfAbsent(checkpointId, (k) -> new HashSet<>());
            tasks.add(task);
            if (tasks.size() == numberOfTasks) {
                notifiedTasks.headMap(checkpointId, true).clear();
                return true;
            }
            return false;
        }
    }

    public class PolicyContext implements PartitionCommitPolicy.Context {

        private final List<String> partitionValues;
        private final Path partitionPath;
		private Set<String> openPartitions;
		private Set<String> committedPartitions;
		private long checkpointId;
		private boolean eagerCommit;

        private PolicyContext(List<String> partitionValues, Path partitionPath) {
            this.partitionValues = partitionValues;
            this.partitionPath = partitionPath;
        }

		private PolicyContext(List<String> partitionValues, Path partitionPath, boolean eagerCommit) {
			this.partitionValues = partitionValues;
			this.partitionPath = partitionPath;
			this.eagerCommit = eagerCommit;
		}


		private PolicyContext(List<String> partitionValues, Path partitionPath, Set<String> openPartitions,
							  Set<String> committedPartitions, long checkpointId) {
			this.partitionValues = partitionValues;
			this.partitionPath = partitionPath;
			this.openPartitions = openPartitions;
			this.committedPartitions = committedPartitions;
			this.checkpointId = checkpointId;
		}

		private PolicyContext(List<String> partitionValues, Path partitionPath, Set<String> openPartitions,
							  Set<String> committedPartitions, long checkpointId, boolean eagerCommit) {
			this.partitionValues = partitionValues;
			this.partitionPath = partitionPath;
			this.openPartitions = openPartitions;
			this.committedPartitions = committedPartitions;
			this.checkpointId = checkpointId;
			this.eagerCommit = eagerCommit;
		}

        @Override
        public String catalogName() {
            return tableIdentifier.getCatalogName();
        }

        @Override
        public String databaseName() {
            return tableIdentifier.getDatabaseName();
        }

        @Override
        public String tableName() {
            return tableIdentifier.getObjectName();
        }

        @Override
        public List<String> partitionKeys() {
            return partitionKeys;
        }

        @Override
        public List<String> partitionValues() {
            if (pathContainPartitionKey) {
                return new ArrayList<>(extractPartitionSpecFromPath(partitionPath).values());
            } else {
                return new ArrayList<>(extractPartitionSpecFromPathWithoutPartitionKey(partitionKeys, partitionPath).values());
            }
        }

        @Override
        public Path partitionPath() {
            return partitionPath;
        }

        @Override
        public String owner() {
            return system_user_id;
        }

		@Override
		public boolean eagerCommit() {
			return eagerCommit;
		}

		public String logId() {
            return  conf.getString(SINK_PARTITION_TRACE_ID);
        }

		@Override
		public Set<String> openPartitions() {
			return this.openPartitions;
		}

		@Override
		public Set<String> committedPartitions() {
			return this.committedPartitions;
		}

		@Override
		public boolean isAllPartitionsReady() {
			if (!defaultPartitionCheck) {
				//dont need partition ready check
				return true;
			}
			List<String> openPartitions = getFilterPartition(this.openPartitions);
			List<String> commitPartition = getFilterPartition(this.committedPartitions);
			LOG.info("ready judge list open :{}, commit :{}, partition :{}, partitionValues :{}", openPartitions,
				commitPartition, this.partitionPath, this.partitionValues);
			return openPartitions.size() == commitPartition.size() && allShouldClose(commitPartition);
		}

		private boolean allShouldClose(List<String> commitPartitions) {
			for (String commitPartition : commitPartitions) {
				if (!trigger.shouldClosed(checkpointId, commitPartition)) {
					return false;
				}
			}
			return true;
		}

		private List<String> getFilterPartition(Set<String> partitionList) {
			List<String> currentPartition = partitionValues();
			return partitionList.stream().filter(e -> {
				List<String> partition = pathContainPartitionKey ?
					new ArrayList<>(extractPartitionSpecFromPath(new Path(e)).values()) : new ArrayList<>
					(extractPartitionSpecFromPathWithoutPartitionKey(partitionKeys, new Path(e)).values());
				return PartPathUtils.isHourPath(partitionKeys) ?
					partition.get(0).equals(currentPartition.get(0))
						&& partition.get(1).equals(currentPartition.get(1)) :
					partition.get(0).equals(currentPartition.get(0));
			}).collect(Collectors.toList());
		}
	}
}
