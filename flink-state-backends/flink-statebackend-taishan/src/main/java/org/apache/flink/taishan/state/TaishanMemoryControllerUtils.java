package org.apache.flink.taishan.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.taishan.state.cache.*;

/**
 * @author Dove
 * @Date 2022/10/11 2:35 下午
 */
public class TaishanMemoryControllerUtils {
	private static final int OFF_HEAP_MIN_SEGMENT_COUNT = 16;

	/**
	 * Allocate memory controllable Taishan shared resources.
	 *
	 * @param totalMemorySize The total memory limit size.
	 * @param writeBufferRatio The ratio of total memory which is occupied by write buffer manager.
	 * @param numberOfKeyGroups
	 * @param metricGroup
	 * @return memory controllable Taishan shared resources.
	 */
	public static TaishanSharedResources allocateTaishanSharedResources(
		long totalMemorySize,
		double writeBufferRatio,
		ExecutionConfig executionConfig,
		int numberOfKeyGroups,
		CacheConfiguration cacheConfiguration,
		MetricGroup metricGroup) {
		long calculatedCacheCapacity =
			TaishanMemoryControllerUtils.calculateActualCacheCapacity(
				totalMemorySize, writeBufferRatio);
		int segmentCount = Math.max(Runtime.getRuntime().availableProcessors() * 2, Math.max(numberOfKeyGroups, OFF_HEAP_MIN_SEGMENT_COUNT));

		boolean offHeapTtlEnable = cacheConfiguration.getOffHeapTtlEnable();
		int cacheBatchWriteInterval = cacheConfiguration.getCacheBatchWriteInterval();

		Tuple2<Long, Long> tuple2 = recalculateMemory(calculatedCacheCapacity, cacheConfiguration, executionConfig.getParallelism());
		long offHeapKVSize = tuple2.f0;
		long offHeapAllKeysSize = tuple2.f1;

		// 1) OffHeap KV Cache
		KryoSerializer<CacheEntry> dataValueSerializer = new KryoSerializer<>(CacheEntry.class, executionConfig);
		TCacheOffHeap.DataValueSerializer valueSerializer = new TCacheOffHeap.DataValueSerializer(dataValueSerializer, cacheBatchWriteInterval);
		TCacheOffHeap<CacheEntry> offCacheKV = new TCacheOffHeap(valueSerializer, offHeapKVSize, segmentCount, offHeapTtlEnable);
		initMetricGroup(metricGroup, offCacheKV, false);

		// 2) OffHeap All Keys Cache
		TCacheOffHeap<Integer> offCacheAllKeys = null;
		if (offHeapAllKeysSize != 0) {
			IntSerializer intSerializer = new IntSerializer();
			TCacheOffHeap.VIntSerializer vIntSerializer = new TCacheOffHeap.VIntSerializer(intSerializer, cacheBatchWriteInterval);
			offCacheAllKeys = new TCacheOffHeap(vIntSerializer, offHeapAllKeysSize, segmentCount, offHeapTtlEnable);
			initMetricGroup(metricGroup, offCacheAllKeys, true);
		}

		return new TaishanSharedResources(offCacheKV, offCacheAllKeys, segmentCount);
	}

	private static Tuple2<Long, Long> recalculateMemory(long calculatedCacheCapacity, CacheConfiguration cacheConfiguration, int parallelism) {
		boolean isStoreAllKey = cacheConfiguration.isAllKeyBF();
		Double offHeapBloomRatio = cacheConfiguration.getOffHeapBloomRatio();

		long offHeapAllKeysSize = Math.min(
			isStoreAllKey ? (long) (calculatedCacheCapacity * offHeapBloomRatio) : 0L,
			cacheConfiguration.getOffHeapBloomAllKeyMaxSize() / parallelism
		);

		long offHeapKVSize = calculatedCacheCapacity - offHeapAllKeysSize;
		return new Tuple2(Long.valueOf(offHeapKVSize), Long.valueOf(offHeapAllKeysSize));
	}

	private static void initMetricGroup(MetricGroup metricGroup, TCacheOffHeap offCache, boolean isAllKey) {
		String groupName = isAllKey ? "offHeapAllKey" : "offHeap";
		MetricGroup offHeap = metricGroup.addGroup(groupName);
		offHeap.gauge("size", () -> offCache.size());
		offHeap.gauge("capacity", () -> offCache.capacity());
		offHeap.gauge("freeCapacity", () -> offCache.freeCapacity());
		offHeap.gauge("memUsed", () -> offCache.memUsed());
		offHeap.gauge("hitCount", () -> offCache.hitCount());
		offHeap.gauge("missCount", () -> offCache.missCount());
		offHeap.gauge("evictedEntries", () -> offCache.evictedEntries());
		offHeap.gauge("putAddCount", () -> offCache.putAddCount());
		offHeap.gauge("expiredEntries", () -> offCache.expiredEntries());
		offHeap.gauge("rehashes", () -> offCache.rehashes());
		offHeap.gauge("compactionTimeOut", () -> offCache.getTimeOutsCompactionNum());
		offHeap.gauge("maybeCompactNum", () -> offCache.getMaybeCompactNum());
	}

	static long calculateActualCacheCapacity(long totalMemorySize, double writeBufferRatio) {
		return (long) ((3 - writeBufferRatio) * totalMemorySize / 3);
	}
}
