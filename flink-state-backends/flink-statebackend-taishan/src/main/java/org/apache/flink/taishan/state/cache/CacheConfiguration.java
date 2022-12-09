package org.apache.flink.taishan.state.cache;

import org.apache.flink.configuration.ReadableConfig;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Locale;

import static org.apache.flink.taishan.state.cache.CacheOptions.*;

public class CacheConfiguration implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The maximum fraction of the total shared memory consumed by the write buffers. Null if not set. */
	@Nullable
	private Double writeBufferRatio;

	@Nullable
	private TCache.HeapType heapType;

	private Integer cacheBatchWriteInterval;
	private Integer expireAfter;
	private Integer cacheSize;

	private Integer cacheBatchWriteSize;

	private Boolean bloomFilterEnabled;

	private Boolean dynamicCache;

	private Double maxHeapUsage;

	private Long maxGcTime;

	private Integer cacheBlockingQueueSize;

	private Integer numberOfBloomFilter;

	private Boolean offHeapTtlEnable;

	private Double offHeapBloomRatio;

	private Long offHeapBloomAllKeyMaxSize;

	@Nullable
	public double getWriteBufferRatio() {
		return writeBufferRatio;
	}

	@Nullable
	public TCache.HeapType getHeapType() {
		return heapType;
	}

	public int getCacheBatchWriteInterval() {
		return cacheBatchWriteInterval;
	}

	public int getExpireAfter() {
		return expireAfter;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public int getCacheBatchWriteSize() {
		return cacheBatchWriteSize;
	}

	public boolean getBloomFilterEnabled() {
		return bloomFilterEnabled;
	}

	public boolean getDynamicCache() {
		return dynamicCache;
	}

	public double getMaxHeapUsage() {
		return maxHeapUsage;
	}

	public long getMaxGcTime() {
		return maxGcTime;
	}

	public int getCacheBlockingQueueSize() {
		return cacheBlockingQueueSize;
	}

	public int getNumberOfBloomFilter() {
		return numberOfBloomFilter;
	}

	public boolean isAllKeyBF() {
		return isOffHeap() && getBloomFilterEnabled() && numberOfBloomFilter == 0;
	}

	public boolean isOffHeap() {
		return heapType.equals(TCache.HeapType.OFFHEAP);
	}

	public Double getOffHeapBloomRatio() {
		return offHeapBloomRatio;
	}

	public Long getOffHeapBloomAllKeyMaxSize() {
		return offHeapBloomAllKeyMaxSize;
	}

	public boolean isCacheEnable() {
		return heapType.equals(TCache.HeapType.OFFHEAP) || heapType.equals(TCache.HeapType.HEAP);
	}

	public Boolean getOffHeapTtlEnable() {
		return offHeapTtlEnable;
	}

	public static CacheConfiguration fromOtherAndConfiguration(
		CacheConfiguration other,
		ReadableConfig config) {

		final CacheConfiguration newConfig = new CacheConfiguration();

		newConfig.heapType = other.heapType != null
			? other.heapType
			: TCache.HeapType.valueOf(config.get(CacheOptions.CACHE_TYPE).toUpperCase(Locale.ROOT));
		newConfig.writeBufferRatio = other.writeBufferRatio != null
			? other.writeBufferRatio
			: config.get(CacheOptions.CACHE_OFF_HEAP_BUFFER_RATIO);

		newConfig.cacheBatchWriteInterval = other.cacheBatchWriteInterval != null ?
			other.cacheBatchWriteInterval : config.get(BATCH_WRITE_INTERVAL);
		newConfig.expireAfter = other.expireAfter != null ?
			other.expireAfter : config.get(EXPIRE_AFTER);
		newConfig.cacheSize = other.cacheSize != null ?
			other.cacheSize : config.get(MAX_CACHE_SIZE);
		newConfig.cacheBatchWriteSize = other.cacheBatchWriteSize != null ?
			other.cacheBatchWriteSize : config.get(BATCH_WRITE_COUNT);
		newConfig.cacheBlockingQueueSize = other.cacheBlockingQueueSize != null ?
			other.cacheBlockingQueueSize : config.get(BATCH_WRITE_QUEUE_SIZE);
		newConfig.dynamicCache = other.dynamicCache != null ?
			other.dynamicCache : config.get(CACHE_DYNAMIC_ENABLED);
		newConfig.bloomFilterEnabled = other.bloomFilterEnabled != null ?
			other.bloomFilterEnabled : config.get(BLOOM_FILTER_ENABLED);
		newConfig.numberOfBloomFilter = other.numberOfBloomFilter != null ?
			other.numberOfBloomFilter : config.get(BLOOM_FILTER_NUMBER);
		newConfig.offHeapBloomRatio = other.offHeapBloomRatio != null ?
			other.offHeapBloomRatio : config.get(BLOOM_FILTER_ALL_KEYS_RATIO);
		newConfig.offHeapBloomAllKeyMaxSize = other.offHeapBloomAllKeyMaxSize != null ?
			other.offHeapBloomAllKeyMaxSize : config.get(BLOOM_FILTER_ALL_KEYS_MAX_SIZE);
		newConfig.maxGcTime = other.maxGcTime != null ?
			other.maxGcTime : config.get(CACHE_MAX_GC_TIME);
		newConfig.maxHeapUsage = other.maxHeapUsage != null ?
			other.maxHeapUsage : config.get(CACHE_MAX_HEAP_USAGE);
		newConfig.offHeapTtlEnable = other.offHeapTtlEnable != null ?
			other.offHeapTtlEnable : config.get(CACHE_OFF_HEAP_TTL_ENABLE);

		return newConfig;
	}
}
