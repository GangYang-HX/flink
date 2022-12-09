package org.apache.flink.taishan.state;

import org.apache.flink.taishan.state.cache.CacheEntry;
import org.apache.flink.taishan.state.cache.TCacheOffHeap;

/**
 * @author Dove
 * @Date 2022/10/11 2:36 下午
 */
public class TaishanSharedResources implements AutoCloseable {
	private final TCacheOffHeap<CacheEntry> cacheKV;
	private final TCacheOffHeap<Integer> cacheAllKeys;
	private final long offHeapCapacityKV;
	private final long offHeapCapacityAllKeys;
	private final int segmentCount;

	public TaishanSharedResources(TCacheOffHeap<CacheEntry> cacheKV, TCacheOffHeap<Integer> cacheAllKeys, int segmentCount) {
		this.cacheKV = cacheKV;
		this.cacheAllKeys = cacheAllKeys;
		this.offHeapCapacityKV = cacheKV == null ? 0L : cacheKV.offHeapCapacityBytes();
		this.offHeapCapacityAllKeys = cacheAllKeys == null ? 0L : cacheAllKeys.offHeapCapacityBytes();
		this.segmentCount = segmentCount;
	}

	public TCacheOffHeap<CacheEntry> getCacheKV() {
		return cacheKV;
	}

	public TCacheOffHeap<Integer> getCacheAllKeys() {
		return cacheAllKeys;
	}

	public long getOffHeapCapacityKV() {
		return offHeapCapacityKV;
	}

	public long getOffHeapCapacityAllKeys() {
		return offHeapCapacityAllKeys;
	}

	public int getSegmentCount() {
		return segmentCount;
	}

	@Override
	public void close() throws Exception {
		cacheKV.close();
		cacheAllKeys.close();
	}
}
