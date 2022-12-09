package org.apache.flink.taishan.state;

import com.bilibili.taishan.ClientManager;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.*;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.taishan.state.client.TaishanAdminConfiguration;
import org.apache.flink.taishan.state.client.TaishanClientConfiguration;
import org.apache.flink.taishan.state.bloomfilter.BloomFilterStore;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.cache.CacheBufferGroup;
import org.apache.flink.taishan.state.client.TaishanClientManager;
import org.apache.flink.taishan.state.restore.AbstractTaishanRestoreOperation;
import org.apache.flink.taishan.state.restore.TaishanNoneRestoreOperation;
import org.apache.flink.taishan.state.restore.TaishanNormalRestoreOperation;
import org.apache.flink.taishan.state.restore.TaishanRestoreResult;
import org.apache.flink.taishan.state.serialize.TaishanKeySerializationUtils;
import org.apache.flink.taishan.state.snapshot.TaishanSnapshotStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * @author Dove
 * @Date 2022/6/20 4:08 下午
 */
public class TaishanKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanKeyedStateBackendBuilder.class);

	protected final LocalRecoveryConfig localRecoveryConfig;
	protected final TaskInfo taskInfo;
	protected final JobID jobID;
	protected final ClientManager clientManager;
	protected final MetricGroup metricGroup;
	protected final HeapPriorityQueueSetFactory priorityQueueSetFactory;
	protected final GlobalAggregateManager globalAggregateManager;

	private final CacheConfiguration cacheConfiguration;
	private final OpaqueMemoryResource<TaishanSharedResources> sharedResources;

	private TaishanAdminConfiguration taishanAdminConfiguration;
	private TaishanClientConfiguration taishanClientConfiguration;
	private final TaishanStateBackend.PriorityQueueStateType priorityQueueStateType;

	public TaishanKeyedStateBackendBuilder(TaskKvStateRegistry kvStateRegistry,
										   TypeSerializer keySerializer,
										   ClassLoader userCodeClassLoader,
										   int numberOfKeyGroups,
										   KeyGroupRange keyGroupRange,
										   ExecutionConfig executionConfig,
										   TaishanStateBackend.PriorityQueueStateType priorityQueueStateType,
										   TtlTimeProvider ttlTimeProvider,
										   @Nonnull Collection<KeyedStateHandle> stateHandles,
										   StreamCompressionDecorator keyGroupCompressionDecorator,
										   CloseableRegistry cancelStreamRegistry,
										   LocalRecoveryConfig localRecoveryConfig,
										   JobID jobID,
										   TaskInfo taskInfo,
										   TaishanAdminConfiguration taishanAdminConfiguration,
										   TaishanClientConfiguration taishanClientConfiguration,
										   MetricGroup metricGroup,
										   CacheConfiguration cacheConfiguration,
										   HeapPriorityQueueSetFactory priorityQueueSetFactory,
										   GlobalAggregateManager globalAggregateManager,
										   OpaqueMemoryResource<TaishanSharedResources> sharedResources) {
		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig, ttlTimeProvider, stateHandles, keyGroupCompressionDecorator, cancelStreamRegistry);
		this.localRecoveryConfig = localRecoveryConfig;
		this.jobID = jobID;
		this.taskInfo = taskInfo;
		this.clientManager = TaishanClientManager.getInstance(taishanClientConfiguration);
		this.metricGroup = metricGroup;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.globalAggregateManager = globalAggregateManager;
		this.cacheConfiguration = cacheConfiguration;
		this.taishanAdminConfiguration = taishanAdminConfiguration;
		this.taishanClientConfiguration = taishanClientConfiguration;
		this.priorityQueueStateType = priorityQueueStateType;
		this.sharedResources = sharedResources;
	}

	@Override
	public AbstractKeyedStateBackend build() throws BackendBuildingException {
		CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();

		int keyGroupPrefixBytes = TaishanKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
		LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation = new LinkedHashMap<>();
		Map<String, TaishanMetric> stateTaishanMetricMap = new HashMap<>();

		BloomFilterStore bloomFilterStore = new BloomFilterStore(cacheConfiguration, sharedResources);
		AbstractTaishanRestoreOperation<K> restoreOperation = getTaishanRestoreOperation(kvStateInformation, bloomFilterStore, keyGroupPrefixBytes);

		TaishanRestoreResult restoreResult;
		CacheBufferGroup cacheBufferGroup = new CacheBufferGroup(cacheConfiguration);
		try {
			restoreResult = restoreOperation.restore();
		} catch (Exception e) {
			LOG.error("restore taishan client error:{}", e);
			// Log and rethrow
			if (e instanceof BackendBuildingException) {
				throw (BackendBuildingException) e;
			} else {
				String errMsg = "Caught unexpected exception.";
				LOG.error(errMsg, e);
				throw new BackendBuildingException(errMsg, e);
			}
		}

		InternalKeyContext<K> keyContext = new InternalKeyContextImpl<>(
			keyGroupRange,
			numberOfKeyGroups
		);

		// init snapshot strategy after taishan client is assured to be initialized
		TaishanSnapshotStrategy<K> taishanSnapshotStrategy = new TaishanSnapshotStrategy<>(
			keySerializerProvider.currentSchemaSerializer(),
			localRecoveryConfig,
			keyGroupRange,
			keyGroupCompressionDecorator,
			cancelStreamRegistryForBackend,
			restoreResult,
			kvStateInformation,
			bloomFilterStore,
			restoreOperation.getTaishanAdminConfiguration());
		// Map of registered priority queue set states
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates = new HashMap<>();

		// init priority queue factory
		PriorityQueueSetFactory priorityQueueFactory = initPriorityQueueFactory(
			keyGroupPrefixBytes,
			kvStateInformation,
			restoreResult,
			metricGroup,
			cacheConfiguration,
			cacheBufferGroup,
			sharedResources);

		TaishanKeyedStateBackend<K> stateBackend = new TaishanKeyedStateBackend<K>(
			kvStateRegistry,
			this.keySerializerProvider.currentSchemaSerializer(),
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistryForBackend,
			keyContext,
			jobID,
			operatorID,
			subTaskIndex,
			clientManager,
			taishanSnapshotStrategy,
			keyGroupPrefixBytes,
			restoreResult,
			registeredPQStates,
			priorityQueueSetFactory,
			globalAggregateManager,
			priorityQueueFactory,
			kvStateInformation,
			sharedResources,
			stateTaishanMetricMap);
		stateBackend.setMetricGroup(metricGroup);
		stateBackend.setTaishanAdminConfiguration(restoreOperation.getTaishanAdminConfiguration());
		stateBackend.setTaishanClientConfiguration(taishanClientConfiguration);
		stateBackend.setCacheGroup(cacheBufferGroup);
		stateBackend.setCacheConfiguration(cacheConfiguration);
		stateBackend.setBloomFilterStore(bloomFilterStore);
		return stateBackend;
	}

	private AbstractTaishanRestoreOperation<K> getTaishanRestoreOperation(
		LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation,
		BloomFilterStore bloomFilterStore,
		int keyGroupPrefixBytes) {
		if (restoreStateHandles.isEmpty()) {
			return new TaishanNoneRestoreOperation<>(clientManager, userCodeClassLoader, keySerializerProvider, kvStateInformation, taishanAdminConfiguration);
		} else {
			final TaishanKeyedStateHandle theFirstStateHandle = (TaishanKeyedStateHandle) restoreStateHandles.iterator().next();
			if (theFirstStateHandle == null) {
				LOG.info("Restore from empty keyed state.");
				return new TaishanNoneRestoreOperation<>(clientManager, userCodeClassLoader, keySerializerProvider, kvStateInformation, taishanAdminConfiguration);
			}

			return new TaishanNormalRestoreOperation(clientManager,
				keyGroupRange,
				cancelStreamRegistry,
				userCodeClassLoader,
				keySerializerProvider,
				restoreStateHandles,
				metricGroup,
				kvStateInformation,
				taishanClientConfiguration,
				bloomFilterStore,
				keyGroupPrefixBytes,
				cacheConfiguration,
				sharedResources);
		}
	}

	private PriorityQueueSetFactory initPriorityQueueFactory(
		int keyGroupPrefixBytes,
		Map<String, RegisteredStateMetaInfoBase> kvStateInformation,
		TaishanRestoreResult restoreResult,
		MetricGroup metricGroup,
		CacheConfiguration cacheConfiguration,
		CacheBufferGroup cacheBufferGroup,
		OpaqueMemoryResource<TaishanSharedResources> opaqueMemoryResource) {
		PriorityQueueSetFactory priorityQueueFactory;
		switch (priorityQueueStateType) {
			case HEAP:
				priorityQueueFactory = new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
				break;
			case TAISHAN:
				priorityQueueFactory = new TaishanPriorityQueueSetFactory(
					keyGroupRange,
					keyGroupPrefixBytes,
					numberOfKeyGroups,
					kvStateInformation,
					restoreResult,
					metricGroup,
					executionConfig,
					cacheConfiguration,
					cacheBufferGroup,
					opaqueMemoryResource
				);
				break;
			default:
				throw new IllegalArgumentException("Unknown priority queue state type: " + priorityQueueStateType);
		}
		return priorityQueueFactory;
	}
}
