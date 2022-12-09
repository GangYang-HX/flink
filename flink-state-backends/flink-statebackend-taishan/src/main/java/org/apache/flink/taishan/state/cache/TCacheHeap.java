package org.apache.flink.taishan.state.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.taishan.state.cache.CacheEntry.DONE_ACTION;

/**
 * @author Dove
 * @Date 2022/10/10 8:16 下午
 */
public class TCacheHeap implements TCache<CacheEntry> {
	private final List<Cache<ByteArrayWrapper, CacheEntry>> cache;
	private final int startKeyGroup;
	private final int endKeyGroup;
	private final long maxGcTime;
	private final double maxHeapUsage;
	private final boolean dynamicCache;
	private HeapStatusMonitor heapStatusMonitor;

	public TCacheHeap(int startKeyGroup,
					  int endKeyGroup,
					  CacheConfiguration cacheConfiguration) {
		this.cache = new ArrayList<>();
		this.startKeyGroup = startKeyGroup;
		this.endKeyGroup = endKeyGroup;
		this.heapStatusMonitor = new HeapStatusMonitor();
		int keyGroupSize = endKeyGroup - startKeyGroup + 1;
		for (int i = 0; i < keyGroupSize; i++) {
			Cache<ByteArrayWrapper, CacheEntry> cache = Caffeine.newBuilder()
				.expireAfterAccess(cacheConfiguration.getExpireAfter(), TimeUnit.MINUTES)
				.maximumSize(cacheConfiguration.getCacheSize())
				.build();
			this.cache.add(cache);
		}

		this.maxHeapUsage = cacheConfiguration.getMaxHeapUsage();
		this.maxGcTime = cacheConfiguration.getMaxGcTime();
		this.dynamicCache = cacheConfiguration.getDynamicCache();
		dynamicRemoveElement();
	}

	private void dynamicRemoveElement() {
		if (!dynamicCache) {
			return;
		}
		ScheduledExecutorService heapStatusMonitorThreadPool = Executors.newScheduledThreadPool(1);
		heapStatusMonitorThreadPool.scheduleAtFixedRate(new Runnable() {
															@Override
															public void run() {
																HeapStatusMonitor.MonitorResult monitorResult = heapStatusMonitor.runCheck();
																long totalMemory = monitorResult.getTotalMemory();
																long totalUsedMemory = monitorResult.getTotalUsedMemory();
																double usedMemoryPercentage = (double) totalUsedMemory / (double) totalMemory;
																long gcTime = monitorResult.getGarbageCollectionTime();
																if (gcTime > maxGcTime || usedMemoryPercentage > maxHeapUsage) {
																	for (int i = 0; i < cache.size(); i++) {
																		Cache<ByteArrayWrapper, CacheEntry> cacheClient = cache.get(i);
																		if (cacheClient.policy().eviction().isPresent()) {
																			Map<ByteArrayWrapper, CacheEntry> coldestEntry = cacheClient.policy().eviction().get().coldest(50);
																			for (Map.Entry<ByteArrayWrapper, CacheEntry> cacheEntryEntry : coldestEntry.entrySet()) {
																				if (cacheEntryEntry.getValue().getAction() == DONE_ACTION) {
																					cacheClient.invalidate(cacheEntryEntry.getKey());
																				}
																			}
																		}
																	}
																}

															}
														}, 15,
			1L,
			TimeUnit.MINUTES);
	}

	@Override
	public void put(int keyGroupId, ByteArrayWrapper byteArrayWrapper, CacheEntry cacheEntry, int ttlTime) {
		cache.get(keyGroupId - startKeyGroup).put(byteArrayWrapper, cacheEntry);
	}

	@Override
	public CacheEntry get(int keyGroupId, ByteArrayWrapper byteArrayWrapper) {
		return cache.get(keyGroupId - startKeyGroup).getIfPresent(byteArrayWrapper);
	}

	@Override
	public void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable) {
		cache.get(keyGroupId - startKeyGroup).invalidateAll(iterable);
	}

	@Override
	public boolean isEmpty() {
		for (int i = startKeyGroup; i <= endKeyGroup; i++) {
			for (Map.Entry<ByteArrayWrapper, CacheEntry> cacheEntry : this.cache.get(i - startKeyGroup).asMap().entrySet()) {
				if (cacheEntry != null && cacheEntry.getValue() != null && cacheEntry.getValue().getDataObject() != null) {
					return false;
				}
			}
		}
		return true;
	}

	@Override
	public void close() {
		for (Cache<ByteArrayWrapper, CacheEntry> c : cache) {
			c.cleanUp();
		}
	}
}
