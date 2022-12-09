package org.apache.flink.taishan.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.client.TaishanAdminConfiguration;
import org.apache.flink.taishan.state.client.TaishanClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import static org.apache.flink.taishan.state.configuration.TaishanOptions.TIMER_SERVICE_FACTORY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author Dove
 * @Date 2022/6/20 11:22 上午
 */
public class TaishanStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

	/**
	 * The options to chose for the type of priority queue state.
	 */
	public enum PriorityQueueStateType {
		HEAP,
		TAISHAN
	}

	private static final Logger LOG = LoggerFactory.getLogger(TaishanStateBackend.class);
	/** The state backend that we use for creating checkpoint streams. */
	private final StateBackend checkpointStreamBackend;
	private final CacheConfiguration cacheConfiguration;
	private TaishanAdminConfiguration taishanAdminConfiguration;
	private TaishanClientConfiguration taishanClientConfiguration;

	/** This determines the type of priority queue state. */
	@Nullable
	private PriorityQueueStateType priorityQueueStateType;

	public TaishanStateBackend(String checkpointDataUri) throws IOException {
		this(new Path(checkpointDataUri).toUri());
	}

	public TaishanStateBackend(URI checkpointDataUri) throws IOException {
		this(new FsStateBackend(checkpointDataUri));
	}

	public TaishanStateBackend(StateBackend checkpointStreamBackend) {
		this.checkpointStreamBackend = checkNotNull(checkpointStreamBackend);
		this.cacheConfiguration = new CacheConfiguration();
	}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		return checkpointStreamBackend.resolveCheckpoint(externalPointer);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		return checkpointStreamBackend.createCheckpointStorage(jobId);
	}

	/**
	 * Gets the type of the priority queue state. It will fallback to the default value, if it is not explicitly set.
	 *
	 * @return The type of the priority queue state.
	 */
	public PriorityQueueStateType getPriorityQueueStateType() {
		return priorityQueueStateType == null ? TIMER_SERVICE_FACTORY.defaultValue() : priorityQueueStateType;
	}

	/**
	 * Sets the type of the priority queue state. It will fallback to the default value, if it is not explicitly set.
	 */
	public void setPriorityQueueStateType(PriorityQueueStateType priorityQueueStateType) {
		this.priorityQueueStateType = checkNotNull(priorityQueueStateType);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws IOException {
		return createKeyedStateBackend(
			env,
			jobID,
			operatorIdentifier,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			kvStateRegistry,
			ttlTimeProvider,
			metricGroup,
			stateHandles,
			cancelStreamRegistry,
			null,
			-1,
			1.0);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry,
		OperatorID operatorID,
		int subTaskIndex,
		double managedMemoryFraction) throws IOException {
		ExecutionConfig executionConfig = env.getExecutionConfig();

		final OpaqueMemoryResource<TaishanSharedResources> sharedResources = TaishanOperationUtils
			.allocateSharedCachesIfConfigured(env.getMemoryManager(), managedMemoryFraction, cacheConfiguration, executionConfig, metricGroup, numberOfKeyGroups, LOG);
		logSharedResourcesMessage(sharedResources);
		StreamCompressionDecorator keyGroupCompressionDecorator = getCompressionDecorator(executionConfig);

		LocalRecoveryConfig localRecoveryConfig =
			env.getTaskStateManager().createLocalRecoveryConfig();
		GlobalAggregateManager globalAggregateManager = env.getGlobalAggregateManager();

		HeapPriorityQueueSetFactory priorityQueueSetFactory =
			new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);

		TaishanKeyedStateBackendBuilder builder = new TaishanKeyedStateBackendBuilder(
			kvStateRegistry,
			keySerializer,
			env.getUserClassLoader(),
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			priorityQueueStateType,
			ttlTimeProvider,
			stateHandles,
			keyGroupCompressionDecorator,
			cancelStreamRegistry,
			localRecoveryConfig,
			jobID,
			env.getTaskInfo(),
			taishanAdminConfiguration,
			taishanClientConfiguration,
			metricGroup,
			cacheConfiguration,
			priorityQueueSetFactory,
			globalAggregateManager,
			sharedResources);


		builder.setOperatorID(operatorID)
			.setSubTaskIndex(subTaskIndex);

		return builder.build();
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
		return createOperatorStateBackend(
			env,
			operatorIdentifier,
			stateHandles,
			cancelStreamRegistry,
			null,
			-1);
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry, OperatorID operatorID, int subTaskIndex) throws Exception {
		//the default for Taishan; eventually there can be a operator state backend based on Taishan, too.
		final boolean asyncSnapshots = true;
		return new DefaultOperatorStateBackendBuilder(
			env.getUserClassLoader(),
			env.getExecutionConfig(),
			asyncSnapshots,
			stateHandles,
			cancelStreamRegistry)
			.setOperatorID(operatorID)
			.setSubTaskIndex(subTaskIndex)
			.build();
	}


	@Override
	public TaishanStateBackend configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
		TaishanStateBackend taishanStateBackend = new TaishanStateBackend(this, config, classLoader);
		return taishanStateBackend;
	}

	private TaishanStateBackend(TaishanStateBackend original, ReadableConfig config, ClassLoader classLoader) {
		// reconfigure the state backend backing the streams
		final StateBackend originalStreamBackend = original.checkpointStreamBackend;
		this.checkpointStreamBackend = originalStreamBackend instanceof ConfigurableStateBackend ?
			((ConfigurableStateBackend) originalStreamBackend).configure(config, classLoader) :
			originalStreamBackend;

		// configure others
		this.taishanAdminConfiguration = TaishanAdminConfiguration.loadAdminConfiguration(config);
		this.taishanClientConfiguration = TaishanClientConfiguration.loadClientConfiguration(config);
		this.cacheConfiguration = CacheConfiguration.fromOtherAndConfiguration(original.cacheConfiguration, config);

		if (null == original.priorityQueueStateType) {
			this.priorityQueueStateType = config.get(TIMER_SERVICE_FACTORY);
		} else {
			this.priorityQueueStateType = original.priorityQueueStateType;
		}
	}

	@Override
	public boolean useManagedMemory() {
		return true;
	}

	private void logSharedResourcesMessage(OpaqueMemoryResource<TaishanSharedResources> sharedResources) {
		if (sharedResources != null) {
			TaishanSharedResources resourceHandle = sharedResources.getResourceHandle();
			LOG.info("Obtained shared Taishan cache of size {} bytes, cache heap type:{}, offHeapCapacityKV:{} bytes, offHeapCapacityAllKeys:{} bytes, segmentCount:{}.",
				sharedResources.getSize(), cacheConfiguration.getHeapType(), resourceHandle.getOffHeapCapacityKV(), resourceHandle.getOffHeapCapacityAllKeys(), resourceHandle.getSegmentCount());
		}
	}
}
