package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.TaishanSharedResources;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.cache.TCacheOffHeap;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

public class BloomFilterStore {
	private static final Logger LOG = LoggerFactory.getLogger(BloomFilterStore.class);

	private final OpaqueMemoryResource<TaishanSharedResources> sharedResources;
	public LinkedHashMap<String, BloomFilterClient> blockedBloomFilters;
	private CacheConfiguration cacheConfiguration;

	public BloomFilterStore(CacheConfiguration cacheConfiguration, OpaqueMemoryResource<TaishanSharedResources> sharedResources) {
		this.blockedBloomFilters = new LinkedHashMap<>();
		this.cacheConfiguration = cacheConfiguration;
		this.sharedResources = sharedResources;
	}

	public BloomFilterClient getOrInitFilterClient(String stateName, KeyGroupRange range, int keyGroupPrefixBytes, TaishanClientWrapper taishanClientWrapper) {
		BloomFilterClient client = blockedBloomFilters.get(stateName);
		if (client == null) {
			if (!isEnabled()) {
				client = new TaishanNonBloomFilterClient();
			} else if (cacheConfiguration.isAllKeyBF()) {
				Preconditions.checkNotNull(sharedResources, "Storing All Keys requires allocating the shared OffHeap.");
				TCacheOffHeap<Integer> cache = sharedResources.getResourceHandle().getCacheAllKeys();
				client = new OffHeapBloomFilter(cache, taishanClientWrapper);
			} else if (cacheConfiguration.getNumberOfBloomFilter() == 1) {
				client = new TaishanBloomFilterClient(stateName, range, keyGroupPrefixBytes, taishanClientWrapper);
				client.restore();
			} else {
				client = new SegmentedBloomFilterClient(stateName, range, keyGroupPrefixBytes, cacheConfiguration.getNumberOfBloomFilter(), taishanClientWrapper);
				client.restore();
			}
			blockedBloomFilters.put(stateName, client);
		}
		return client;
	}

	public BloomFilterClient getFilterClient(String stateName) {
		return blockedBloomFilters.get(stateName);
	}

	public void snapshotFilters(String stateName) {
		BloomFilterClient bloomFilterClient = blockedBloomFilters.get(stateName);
		if (bloomFilterClient != null) {
			bloomFilterClient.snapshotFilter();
		}
	}

	public boolean isEnabled() {
		return cacheConfiguration.getBloomFilterEnabled();
	}

	public boolean isAllKeyBF() {
		return cacheConfiguration.isAllKeyBF();
	}

	public void setTaishanMetric(String stateName, TaishanMetric taishanMetric) {
		BloomFilterClient bloomFilterClient = blockedBloomFilters.get(stateName);
		bloomFilterClient.initTaishanMetric(taishanMetric);
	}

	public void setTtlConfig(String stateName, StateTtlConfig ttlConfig) {
		BloomFilterClient client = blockedBloomFilters.get(stateName);
		if (cacheConfiguration.isAllKeyBF() && ttlConfig.isEnabled()) {
			OffHeapBloomFilter offHeapBloomFilter = (OffHeapBloomFilter) client;
			long t = System.currentTimeMillis() + ttlConfig.getTtl().getSize();
			offHeapBloomFilter.setRestoreEarliestAvailableTime(t);
		} else if (cacheConfiguration.isAllKeyBF() && !ttlConfig.isEnabled()) {
			// some state have no ttl. eg:session-window-mapping ttl is not enabled
			LOG.info("state:{} ttl is not enabled, TaishanNonBloomFilter replace OffHeapBloomFilter.", stateName);
			client = new TaishanNonBloomFilterClient();
			blockedBloomFilters.put(stateName, client);
		}
	}
}
