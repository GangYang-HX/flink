package org.apache.flink.taishan.state;

import com.bilibili.taishan.ClientManager;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.*;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.taishan.state.bloomfilter.BloomFilterStore;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.cache.CacheBufferGroup;
import org.apache.flink.taishan.state.cache.SnapshotableCacheClient;
import org.apache.flink.taishan.state.client.*;
import org.apache.flink.taishan.state.restore.TaishanRestoreResult;
import org.apache.flink.taishan.state.serialize.TaishanKeySerializationUtils;
import org.apache.flink.taishan.state.serialize.TaishanSerializedCompositeKeyBuilder;
import org.apache.flink.taishan.state.snapshot.TaishanSnapshotStrategy;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.taishan.state.TaishanMapState.CACHE_SIZE_LIMIT;
import static org.apache.flink.taishan.state.TaishanSnapshotTransformFactoryAdaptor.wrapStateSnapshotTransformFactory;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author Dove
 * @Date 2022/6/20 11:30 上午
 */
public class TaishanKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanKeyedStateBackend.class);

	@SuppressWarnings("deprecation")
	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(MapStateDescriptor.class, (StateFactory) TaishanMapState::create),
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) TaishanValueState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) TaishanListState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) TaishanReducingState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) TaishanAggregatingState::create),
			Tuple2.of(FoldingStateDescriptor.class, (StateFactory) TaishanFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	protected final TaishanSnapshotStrategy<K> snapshotStrategy;
	protected final HashMap<String, TaishanSerializedCompositeKeyBuilder<K>> sharedRangeKeyBuilderMap;
	protected final int keyGroupPrefixBytes;
	protected final TaishanRestoreResult restoreResult;
	private final ClientManager clientManager;
	private final Map<String, TaishanMetric> stateTaishanMetricMap;
	private final GlobalAggregateManager aggregateManager;
	/**
	 * Map of registered priority queue set states.
	 */
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	/**
	 * Factory for state that is organized as priority queue.
	 */
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
	/**
	 * Information about the k/v states, maintained in the order as we create them. This is used to retrieve the
	 * tableName that is used for a state and also for sanity checks when restoring.
	 */
	private final LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation;
	private final JobID jobID;
	/**
	 * Factory for priority queue state.
	 */
	private final PriorityQueueSetFactory priorityQueueFactory;
	private final OpaqueMemoryResource<TaishanSharedResources> sharedResources;

	protected TaishanClientWrapper taishanClientWrapper;
	protected MetricGroup metricGroup;
	private TaishanAdminConfiguration taishanAdminConfiguration;
	private TaishanClientConfiguration taishanClientConfiguration;
	private String taishanTablePrefix = "flink_state_";
	private CacheBufferGroup cacheGroup;
	private BloomFilterStore bloomFilterStore;
	private CacheConfiguration cacheConfiguration;
	private boolean disposed = false;

	public TaishanKeyedStateBackend(TaskKvStateRegistry kvStateRegistry,
									TypeSerializer<K> keySerializer,
									ClassLoader userCodeClassLoader,
									ExecutionConfig executionConfig,
									TtlTimeProvider ttlTimeProvider,
									CloseableRegistry cancelStreamRegistry,
									InternalKeyContext<K> keyContext,
									JobID jobID,
									OperatorID operatorID,
									int subTaskIndex,
									ClientManager clientManager,
									TaishanSnapshotStrategy<K> snapshotStrategy,
									int keyGroupPrefixBytes,
									TaishanRestoreResult restoreResult,
									Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
									HeapPriorityQueueSetFactory priorityQueueSetFactory,
									GlobalAggregateManager globalAggregateManager,
									PriorityQueueSetFactory priorityQueueFactory,
									LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation,
									OpaqueMemoryResource<TaishanSharedResources> sharedResources,
									Map<String, TaishanMetric> stateTaishanMetricMap) {
		super(kvStateRegistry, keySerializer, userCodeClassLoader, executionConfig, ttlTimeProvider, cancelStreamRegistry, keyContext, operatorID, subTaskIndex);
		this.clientManager = clientManager;
		this.snapshotStrategy = snapshotStrategy;
		this.sharedRangeKeyBuilderMap = new HashMap<>();
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.restoreResult = restoreResult;
		this.registeredPQStates = registeredPQStates;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.jobID = jobID;
		this.aggregateManager = globalAggregateManager;
		this.kvStateInformation = kvStateInformation;
		this.stateTaishanMetricMap = stateTaishanMetricMap;
		this.priorityQueueFactory = priorityQueueFactory;
		this.sharedResources = sharedResources;
	}

	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS createState(
			StateDescriptor<S, SV> stateDesc,
			RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
			TaishanKeyedStateBackend<K> backend,
			TaishanMetric taishanMetric) throws Exception;
	}

	@Override
	public int numKeyValueStateEntries() {
		int count = 0;

		for (RegisteredStateMetaInfoBase metaInfoBase : kvStateInformation.values()) {
			String stateName = metaInfoBase.getName();
			TaishanSerializedCompositeKeyBuilder<K> sharedRangeKeyBuilder = sharedRangeKeyBuilderMap.get(stateName);
			byte[] rangeBytes = sharedRangeKeyBuilder.buildCompositeColumnFamily();
			TaishanMetric taishanMetric = getTaishanMetric(stateName);
			SnapshotableCacheClient client = this.cacheGroup.getCacheClient(stateName);
			client.flushBatchWrites();
			TaishanClientIteratorWrapper scan = new TaishanClientIteratorWrapper(taishanClientWrapper, getKeyGroupRange(), rangeBytes, CACHE_SIZE_LIMIT, taishanMetric);
			while (scan.hasNext()) {
				count++;
				scan.next();
			}
		}
		return count;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		snapshotStrategy.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		snapshotStrategy.notifyCheckpointAborted(checkpointId);
	}

	@Override
	public <N> Stream getKeys(String state, N namespace) {
		RegisteredStateMetaInfoBase metaInfoBase = kvStateInformation.get(state);
		if (!(metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo)) {
			return Stream.empty();
		}

		RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo = (RegisteredKeyValueStateBackendMetaInfo<N, ?>) metaInfoBase;

		final TypeSerializer<N> namespaceSerializer = registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
		final DataOutputSerializer namespaceOutputView = new DataOutputSerializer(8);
		boolean ambiguousKeyPossible = TaishanKeySerializationUtils.isAmbiguousKeyPossible(getKeySerializer(), namespaceSerializer);
		final byte[] nameSpaceBytes;
		try {
			TaishanKeySerializationUtils.writeNameSpace(
				namespace,
				namespaceSerializer,
				namespaceOutputView,
				ambiguousKeyPossible);
			nameSpaceBytes = namespaceOutputView.getCopyOfBuffer();
		} catch (IOException ex) {
			throw new FlinkRuntimeException("Failed to get keys from Taishan state backend.", ex);
		}

		TaishanSerializedCompositeKeyBuilder serializedCompositeKeyBuilder = new TaishanSerializedCompositeKeyBuilder<>(
			state,
			keySerializer,
			keyGroupPrefixBytes,
			32);
		serializedCompositeKeyBuilder.setColumnFamily(state);
		int columnFamilyLength = serializedCompositeKeyBuilder.getColumnFamilyLength();
		byte[] columnFamilyBytes = serializedCompositeKeyBuilder.buildCompositeColumnFamily();

		TaishanMetric taishanMetric = getTaishanMetric(state);
		SnapshotableCacheClient client = this.cacheGroup.getCacheClient(state);
		client.flushBatchWrites();

		TaishanClientIteratorWrapper iteratorWrapper =
			new TaishanClientIteratorWrapper(taishanClientWrapper, getKeyGroupRange(), columnFamilyBytes, CACHE_SIZE_LIMIT, taishanMetric);
		LOG.debug("columnFamilyLength:{}, stateName:{}", columnFamilyLength, state);
		final TaishanClientStateKeysIterator<K> stateKeysIterator = new TaishanClientStateKeysIterator<>(iteratorWrapper, state, getKeySerializer(), keyGroupPrefixBytes,
			ambiguousKeyPossible, nameSpaceBytes, columnFamilyLength);

		Stream<K> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(stateKeysIterator, Spliterator.ORDERED), false);
		return targetStream.onClose(stateKeysIterator::close);
	}

	public void setCacheGroup(CacheBufferGroup cacheGroup) {
		this.cacheGroup = cacheGroup;
	}

	public void setCacheConfiguration(CacheConfiguration cacheConfiguration) {
		this.cacheConfiguration = cacheConfiguration;
	}

	public CacheBufferGroup getCacheGroup() {
		return cacheGroup;
	}

	public CacheConfiguration getCacheConfiguration() {
		return cacheConfiguration;
	}

	public void setBloomFilterStore(BloomFilterStore bloomFilterStore) {
		this.bloomFilterStore = bloomFilterStore;
	}

	public BloomFilterStore getBloomFilterStore() {
		return this.bloomFilterStore;
	}

	public TaishanSharedResources getTaishanSharedResources() {
		return sharedResources == null ? null : sharedResources.getResourceHandle();
	}

	@Nonnull
	@Override
	public RunnableFuture snapshot(long checkpointId, long timestamp, @Nonnull CheckpointStreamFactory streamFactory, @Nonnull CheckpointOptions checkpointOptions) throws Exception {
		long startTime = System.currentTimeMillis();

		for (Map.Entry<String, SnapshotableCacheClient> client : this.cacheGroup.taishanClientMap.entrySet()) {
			client.getValue().flushBatchWrites();
		}

		snapshotBloomFilter(bloomFilterStore);

		snapshotStrategy.setStateDescriptors(stateDescriptors);
		snapshotStrategy.setOperatorID(operatorID);
		snapshotStrategy.setSubTaskIndex(subTaskIndex);

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			snapshotStrategy.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

		snapshotStrategy.logSyncCompleted(streamFactory, startTime);
		return snapshotRunner;
	}

	private void snapshotBloomFilter(BloomFilterStore bloomFilterStore) {
		if (bloomFilterStore.isAllKeyBF()) {
			// do nothing
		} else {
			for (Map.Entry<String, RegisteredStateMetaInfoBase> kvStateInfoEntry : kvStateInformation.entrySet()) {
				this.bloomFilterStore.snapshotFilters(kvStateInfoEntry.getKey());
			}
		}
	}

	private synchronized void prepareTaishanClient(String stateName) throws Exception {
		taishanClientWrapper = restoreResult.getTaishanClientWrapper();
		String taishanTableName = getTaishanTableName();
		LOG.info("get or create keyed state from taishan table:{}, state name:{}", taishanTableName, stateName);

		if (restoreResult.getTaishanClientWrapper() == null) {
			// Create the Taishan table only when actually get the state
			CreateTaishanTableFunc.AccInput accInput = new CreateTaishanTableFunc.AccInput(taishanTableName, getNumberOfKeyGroups(), taishanAdminConfiguration);
			aggregateManager.updateGlobalAggregate(CreateTaishanTableFunc.NAME, accInput, new CreateTaishanTableFunc());
			createTaishanTableForUT(aggregateManager, taishanTableName);

			long waitStart = System.currentTimeMillis();
			TaishanHttpUtils.ResponseBody responseBodyQuery = TaishanHttpUtils.queryTaishanTable(taishanAdminConfiguration, taishanTableName, taishanAdminConfiguration.getCreateTableTimeOut());
			long waitEnd = System.currentTimeMillis();
			LOG.info("Waiting taishan Table shard finished:{}, cost {}ms, body message:{}", taishanTableName, waitEnd - waitStart, responseBodyQuery);
			String accessToken = responseBodyQuery.getAccessToken();
			taishanClientWrapper = TaishanClientWrapperUtils.openTaishanClient(clientManager, taishanClientConfiguration, taishanTableName, accessToken);
			LOG.info("Create a Taishan Client wrapper finished.");

			// Reuse client when creating other states
			restoreResult.setTaishanClientWrapper(taishanClientWrapper);
			restoreResult.setAccessToken(accessToken);
			restoreResult.setTaishanTableName(taishanTableName);
		}
		LOG.info("check restoreResult:{}", restoreResult);
	}

	private void createTaishanTableForUT(GlobalAggregateManager aggregateManager, String taishanTableName) {
		String name = aggregateManager.getClass().getName();
		if ("org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager".equals(name)) {
			LOG.debug("[UT] Start creating the Taishan table:{}", taishanTableName);
			long createStart = System.currentTimeMillis();
			TaishanHttpUtils.ResponseBody responseBodyCreate = TaishanHttpUtils.createTaishanTable(taishanAdminConfiguration, taishanTableName, getNumberOfKeyGroups());
			long createEnd = System.currentTimeMillis();
			LOG.debug("[UT] Create taishan table successed:{}, cost {}ms, create Body:{}", taishanTableName, createEnd - createStart, responseBodyCreate);
		}
	}

	private String getTaishanTableName() {
		String newTaishanName = "";
		if (operatorID == null) {
			// For UT
			newTaishanName = taishanTablePrefix + UUID.randomUUID().toString().substring(0, 20).replaceAll("-", "_");
		} else {
			//	taishan tableName length: 12 + 9(jobId) + '_' + 10(operatorId) = 32
			newTaishanName = taishanTablePrefix + jobID.toHexString().substring(0, 9) + "_" + operatorID.toHexString().substring(0, 10);
		}
		return restoreResult.getTaishanTableName() == null ? newTaishanName : restoreResult.getTaishanTableName(); // online
	}

	@Override
	public void dispose() {
		if (this.disposed) {
			return;
		}
		super.dispose();
		for (Map.Entry<String, SnapshotableCacheClient> client : this.cacheGroup.taishanClientMap.entrySet()) {
			client.getValue().close();
		}
		IOUtils.closeQuietly(sharedResources);
		this.disposed = true;
	}

	@Nonnull
	@Override
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		prepareTaishanClient(stateDesc.getName());
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		TaishanMetric taishanMetric = getTaishanMetric(stateDesc.getName());
		RegisteredKeyValueStateBackendMetaInfo<N, SV> registerResult = tryRegisterKvStateInformation(
			stateDesc, namespaceSerializer, snapshotTransformFactory);

		bloomFilterStore.getOrInitFilterClient(stateDesc.getName(), keyGroupRange, keyGroupPrefixBytes, taishanClientWrapper);
		bloomFilterStore.setTtlConfig(stateDesc.getName(), stateDesc.getTtlConfig());
		bloomFilterStore.setTaishanMetric(stateDesc.getName(), taishanMetric);
		// snapshotTransformFactory
		return stateFactory.createState(stateDesc, registerResult, TaishanKeyedStateBackend.this, taishanMetric);
	}

	private TaishanMetric getTaishanMetric(String stateName) {
		return TaishanMetric.getOrCreateTaishanMetric(stateTaishanMetricMap, stateName, metricGroup, getExecutionConfig().getRocksDBMetricSampleRate(), cacheConfiguration);
	}

	/**
	 * Registers a k/v state information, which includes its state id, type, Taishan column family handle, and serializers.
	 *
	 * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the global Taishan database and
	 * the list of k/v state information. When a k/v state is first requested we check here whether we
	 * already have a registered entry for that and return it (after some necessary state compatibility checks)
	 * or create a new one if it does not exist.
	 */
	private <N, S extends State, SV, SEV> RegisteredKeyValueStateBackendMetaInfo<N, SV> tryRegisterKvStateInformation(
		StateDescriptor<S, SV> stateDesc,
		TypeSerializer<N> namespaceSerializer,
		@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {

		RegisteredStateMetaInfoBase oldStateInfo = kvStateInformation.get(stateDesc.getName());

		TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

		RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
		if (oldStateInfo != null) {
			@SuppressWarnings("unchecked")
			RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo = (RegisteredKeyValueStateBackendMetaInfo) oldStateInfo;

			newMetaInfo = updateRestoredStateMetaInfo(
				castedMetaInfo,
				stateDesc,
				namespaceSerializer,
				stateSerializer);

			kvStateInformation.put(stateDesc.getName(), newMetaInfo);
		} else {
			newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				stateSerializer,
				StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

			kvStateInformation.put(stateDesc.getName(), newMetaInfo);
		}

		StateSnapshotTransformer.StateSnapshotTransformFactory<SV> wrappedSnapshotTransformFactory = wrapStateSnapshotTransformFactory(
			stateDesc, snapshotTransformFactory, newMetaInfo.getStateSerializer());
		newMetaInfo.updateSnapshotTransformFactory(wrappedSnapshotTransformFactory);

		return newMetaInfo;
	}

	private <N, S extends State, SV> RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
		RegisteredKeyValueStateBackendMetaInfo<N, SV> oldStateInfo,
		StateDescriptor<S, SV> stateDesc,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer) throws Exception {

		@SuppressWarnings("unchecked")
		RegisteredKeyValueStateBackendMetaInfo<N, SV> restoredKvStateMetaInfo = oldStateInfo;

		TypeSerializerSchemaCompatibility<N> s = restoredKvStateMetaInfo.updateNamespaceSerializer(namespaceSerializer);
		if (s.isCompatibleAfterMigration() || s.isIncompatible()) {
			throw new StateMigrationException("The new namespace serializer must be compatible.");
		}

		restoredKvStateMetaInfo.checkStateMetaInfo(stateDesc);

		TypeSerializerSchemaCompatibility<SV> newStateSerializerCompatibility =
			restoredKvStateMetaInfo.updateStateSerializer(stateSerializer);
		if (newStateSerializerCompatibility.isCompatibleAfterMigration()) {
			migrateStateValues(stateDesc, oldStateInfo);
		} else if (newStateSerializerCompatibility.isIncompatible()) {
			throw new StateMigrationException("The new state serializer cannot be incompatible.");
		}

		return restoredKvStateMetaInfo;
	}

	/**
	 * Migrate only the state value, that is the "value" that is stored in Taishan. We don't migrate
	 * the key here, which is made up of key group, key, namespace and map key
	 * (in case of MapState).
	 */
	@SuppressWarnings("unchecked")
	private <N, S extends State, SV> void migrateStateValues(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> stateMetaInfo) throws Exception {

		if (stateDesc.getType() == StateDescriptor.Type.MAP) {
			TypeSerializerSnapshot<SV> previousSerializerSnapshot = stateMetaInfo.getPreviousStateSerializerSnapshot();
			checkState(previousSerializerSnapshot != null, "the previous serializer snapshot should exist.");
			checkState(previousSerializerSnapshot instanceof MapSerializerSnapshot, "previous serializer snapshot should be a MapSerializerSnapshot.");

			TypeSerializer<SV> newSerializer = stateMetaInfo.getStateSerializer();
			checkState(newSerializer instanceof MapSerializer, "new serializer should be a MapSerializer.");

			MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) newSerializer;
			MapSerializerSnapshot<?, ?> mapSerializerSnapshot = (MapSerializerSnapshot<?, ?>) previousSerializerSnapshot;
			if (!checkMapStateKeySchemaCompatibility(mapSerializerSnapshot, mapSerializer)) {
				throw new StateMigrationException(
					"The new serializer for a MapState requires state migration in order for the job to proceed, since the key schema has changed. However, migration for MapState currently only allows value schema evolutions.");
			}
		}

		LOG.info(
			"Performing state migration for state {} because the state serializer's schema, i.e. serialization format, has changed.",
			stateDesc);

		// we need to get an actual state instance because migration is different
		// for different state types. For example, ListState needs to deal with
		// individual elements
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		State state = stateFactory.createState(
			stateDesc,
			stateMetaInfo,
			TaishanKeyedStateBackend.this,
			null);
		if (!(state instanceof AbstractTaishanState)) {
			throw new FlinkRuntimeException(
				"State should be an AbstractTaishanState but is " + state);
		}
	}

	@SuppressWarnings("unchecked")
	private static <UK> boolean checkMapStateKeySchemaCompatibility(
		MapSerializerSnapshot<?, ?> mapStateSerializerSnapshot,
		MapSerializer<?, ?> newMapStateSerializer) {
		TypeSerializerSnapshot<UK> previousKeySerializerSnapshot = (TypeSerializerSnapshot<UK>) mapStateSerializerSnapshot.getKeySerializerSnapshot();
		TypeSerializer<UK> newUserKeySerializer = (TypeSerializer<UK>) newMapStateSerializer.getKeySerializer();

		TypeSerializerSchemaCompatibility<UK> keyCompatibility = previousKeySerializerSnapshot.resolveSchemaCompatibility(newUserKeySerializer);
		return keyCompatibility.isCompatibleAsIs();
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(@Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		try {
			prepareTaishanClient(stateName);
		} catch (Exception e) {
			LOG.error("init taishanClient failed, message: {}.", e.getMessage());
		}
		return priorityQueueFactory.create(stateName, byteOrderedElementSerializer);
	}

	@Nonnull
	private <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> createInternal(
		RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) {

		final String stateName = metaInfo.getName();
		final HeapPriorityQueueSet<T> priorityQueue = priorityQueueSetFactory.create(
			stateName,
			metaInfo.getElementSerializer());

		HeapPriorityQueueSnapshotRestoreWrapper<T> wrapper =
			new HeapPriorityQueueSnapshotRestoreWrapper<>(
				priorityQueue,
				metaInfo,
				KeyExtractorFunction.forKeyedObjects(),
				keyGroupRange,
				numberOfKeyGroups);

		registeredPQStates.put(stateName, wrapper);
		return priorityQueue;
	}

	@Override
	public void setCurrentKey(K newKey) {
		super.setCurrentKey(newKey);
		int keyGroupId = KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups);
		sharedRangeKeyBuilderMap.values().forEach(a -> a.setKey(newKey, keyGroupId));
	}

	@Override
	public boolean supportsAsynchronousSnapshots() {
		return true;
	}

	/**
	 * Taishan State not supported OnReadAndWrite ttl.
	 */
	@Override
	public boolean isSupportedOnReadAndWrite() {
		return false;
	}

	public void setTaishanAdminConfiguration(TaishanAdminConfiguration taishanAdminConfiguration) {
		this.taishanAdminConfiguration = taishanAdminConfiguration;
		// flink_state_
		try {
			this.taishanTablePrefix = taishanTablePrefix.replaceFirst("flink", taishanAdminConfiguration.getGroupName().split("\\.")[1]);
		} catch (Exception e) {
		}
	}

	public void setTaishanClientConfiguration(TaishanClientConfiguration taishanClientConfiguration) {
		this.taishanClientConfiguration = taishanClientConfiguration;
	}

	public int getKeyGroupPrefixBytes() {
		return keyGroupPrefixBytes;
	}

	public void setMetricGroup(MetricGroup metricGroup) {
		this.metricGroup = metricGroup;
	}

	public TaishanSerializedCompositeKeyBuilder<K> getSharedRangeKeyBuilder(String stateName) {
		TaishanSerializedCompositeKeyBuilder<K> sharedRangeKeyBuilder = sharedRangeKeyBuilderMap.getOrDefault(stateName,
			new TaishanSerializedCompositeKeyBuilder<>(
				stateName,
				keySerializer,
				keyGroupPrefixBytes,
				32));
		sharedRangeKeyBuilder.setKey(getCurrentKey(), getCurrentKeyGroupIndex());
		sharedRangeKeyBuilderMap.put(stateName, sharedRangeKeyBuilder);
		return sharedRangeKeyBuilder;
	}
}
