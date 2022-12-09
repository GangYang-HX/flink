package org.apache.flink.taishan.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;
import org.apache.flink.taishan.state.cache.ByteArrayWrapper;
import org.apache.flink.taishan.state.cache.CacheBufferGroup;
import org.apache.flink.taishan.state.cache.CacheBufferClient;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.cache.CacheEntry;
import org.apache.flink.taishan.state.cache.NoneCacheClient;
import org.apache.flink.taishan.state.cache.SnapshotableCacheClient;
import org.apache.flink.taishan.state.cache.TCache;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.taishan.state.restore.TaishanRestoreResult;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the logic and resources in connection with creating priority
 * queue state structures, for Taishan backend.
 */
public class TaishanPriorityQueueSetFactory implements PriorityQueueSetFactory {

	private final KeyGroupRange keyGroupRange;
	private final int keyGroupPrefixBytes;
	private final int numberOfKeyGroups;
	private final Map<String, RegisteredStateMetaInfoBase> kvStateInformation;
	private final TaishanRestoreResult restoreResult;
	private final MetricGroup metricGroup;
	private final Map<String, TaishanMetric> stateTaishanMetricMap;
	private final ExecutionConfig executionConfig;
	private final CacheConfiguration cacheConfiguration;
	private final CacheBufferGroup cacheBufferGroup;
	private final OpaqueMemoryResource<TaishanSharedResources> opaqueMemoryResource;

	TaishanPriorityQueueSetFactory(
		KeyGroupRange keyGroupRange,
		int keyGroupPrefixBytes,
		int numberOfKeyGroups,
		Map<String, RegisteredStateMetaInfoBase> kvStateInformation,
		TaishanRestoreResult restoreResult,
		MetricGroup metricGroup,
		ExecutionConfig executionConfig,
		CacheConfiguration cacheConfiguration,
		CacheBufferGroup cacheBufferGroup,
		OpaqueMemoryResource<TaishanSharedResources> opaqueMemoryResource) {
		this.keyGroupRange = keyGroupRange;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.kvStateInformation = kvStateInformation;
		this.restoreResult = restoreResult;
		this.metricGroup = metricGroup;
		this.stateTaishanMetricMap = new HashMap<>();
		this.executionConfig = executionConfig;
		this.cacheConfiguration = cacheConfiguration;
		this.cacheBufferGroup = cacheBufferGroup;
		this.opaqueMemoryResource = opaqueMemoryResource;
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T>
	create(@Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		TaishanMetric taishanMetric = getTaishanMetric(stateName);
		tryRegisterPriorityQueueMetaInfo(stateName, byteOrderedElementSerializer, taishanMetric);

		TaishanClientWrapper clientWrapper = restoreResult.getTaishanClientWrapper();
		SnapshotableCacheClient cacheClient = getOrCreateCacheClient(stateName, cacheConfiguration, clientWrapper, taishanMetric);
		return new KeyGroupPartitionedPriorityQueue<>(
			KeyExtractorFunction.forKeyedObjects(),
			PriorityComparator.forPriorityComparableObjects(),
			new KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<T, TaishanCachingPriorityQueueSet<T>>() {
				@Nonnull
				@Override
				public TaishanCachingPriorityQueueSet<T> create(
					int keyGroupId,
					int numKeyGroups,
					@Nonnull KeyExtractorFunction<T> keyExtractor,
					@Nonnull PriorityComparator<T> elementPriorityComparator) {
					TreeOrderedSetCache orderedSetCache = new TreeOrderedSetCache(TaishanMapState.CACHE_SIZE_LIMIT);
					return new TaishanCachingPriorityQueueSet<>(
						keyGroupId,
						keyGroupPrefixBytes,
						stateName,
						clientWrapper,
						byteOrderedElementSerializer,
						new DataOutputSerializer(128),
						new DataInputDeserializer(),
						cacheClient,
						orderedSetCache,
						taishanMetric
					);
				}
			},
			keyGroupRange,
			numberOfKeyGroups);
	}

	private SnapshotableCacheClient getOrCreateCacheClient(String stateName, CacheConfiguration cacheConfiguration, TaishanClientWrapper taishanClientWrapper, TaishanMetric taishanMetric) {
		SnapshotableCacheClient cacheClient = cacheBufferGroup.getCacheClient(stateName);
		if (cacheClient == null) {
			TCache.HeapType heapType = cacheConfiguration.getHeapType();
			switch (heapType) {
				case HEAP:
				case OFFHEAP:
					TCache<CacheEntry> emptyCache = new TCache<CacheEntry>() {
						@Override
						public void put(int keyGroupId, ByteArrayWrapper byteArrayWrapper, CacheEntry cacheEntry, int ttlTime) {
							// do nothing
						}

						@Override
						public CacheEntry get(int keyGroupId, ByteArrayWrapper byteArrayWrapper) {
							throw new FlinkRuntimeException("Timer state not supported get api.");
						}

						@Override
						public void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable) {
							throw new FlinkRuntimeException("Timer state not supported removeAll api.");
						}

						@Override
						public boolean isEmpty() {
							throw new FlinkRuntimeException("Timer state not supported isEmpty api.");
						}

						@Override
						public void close() {
						}
					};
					cacheClient = new CacheBufferClient(taishanClientWrapper, cacheConfiguration, keyGroupRange, emptyCache, taishanClientWrapper.getOffHeapKeyPre(), taishanMetric);
					break;
				case NONE:
					cacheClient = new NoneCacheClient(taishanMetric, taishanClientWrapper);
					break;
				default:
					throw new FlinkRuntimeException("Only three heap types are supported(heap/offheap/none).");
			}
			cacheBufferGroup.addCacheTaishanClient(stateName, cacheClient);
		}
		return cacheClient;
	}

	@Nonnull
	private <T> RegisteredPriorityQueueStateBackendMetaInfo<T> tryRegisterPriorityQueueMetaInfo(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer,
		TaishanMetric taishanMetric) {

		RegisteredStateMetaInfoBase stateInfo = kvStateInformation.get(stateName);
		RegisteredPriorityQueueStateBackendMetaInfo<T> newMetaInfo;

		if (stateInfo == null) {
			// Currently this class is for timer service and TTL feature is not applicable here,
			// so no need to register compact filter when creating column family
			newMetaInfo = new RegisteredPriorityQueueStateBackendMetaInfo<>(stateName, byteOrderedElementSerializer);
			kvStateInformation.put(stateName, newMetaInfo);

		} else {
			// TODO we implement the simple way of supporting the current functionality, mimicking keyed state
			// because this should be reworked in FLINK-9376 and then we should have a common algorithm over
			// StateMetaInfoSnapshot that avoids this code duplication.

			newMetaInfo = (RegisteredPriorityQueueStateBackendMetaInfo<T>) stateInfo;

			TypeSerializer<T> previousElementSerializer = newMetaInfo.getPreviousElementSerializer();

			if (previousElementSerializer != byteOrderedElementSerializer) {
				TypeSerializerSchemaCompatibility<T> compatibilityResult =
					newMetaInfo.updateElementSerializer(byteOrderedElementSerializer);

				// Since priority queue elements are written into Taishan
				// as keys prefixed with the key group and namespace, we do not support
				// migrating them. Therefore, here we only check for incompatibility.
				if (compatibilityResult.isIncompatible()) {
					throw new FlinkRuntimeException(
						new StateMigrationException("The new priority queue serializer must not be incompatible."));
				}

				kvStateInformation.put(stateName, newMetaInfo);
			}
		}

		return newMetaInfo;
	}

	private TaishanMetric getTaishanMetric(String stateName) {
		return TaishanMetric.getOrCreateTaishanMetric(stateTaishanMetricMap, stateName, metricGroup, executionConfig.getRocksDBMetricSampleRate(), cacheConfiguration);
	}

}
