package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.cache.ByteArrayWrapper;
import org.apache.flink.taishan.state.cache.TCacheOffHeap;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import static org.apache.flink.taishan.state.cache.OffHeapUtils.cacheKeyWrapper;

/**
 * @author Dove
 * @Date 2022/10/25 7:29 下午
 */
public class OffHeapBloomFilter implements BloomFilterClient {

	private final TCacheOffHeap<Integer> cacheOffHeap;
	private final TaishanClientWrapper taishanClientWrapper;
	private final byte[] offHeapKeyPre;
	private TaishanMetric taishanMetric;
	private boolean isRestore = false;
	private boolean isAvailable = true;
	private long restoreEarliestAvailableTime = 0L;

	public OffHeapBloomFilter(TCacheOffHeap<Integer> cacheOffHeap,
							  TaishanClientWrapper taishanClientWrapper) {
		this.cacheOffHeap = cacheOffHeap;
		this.taishanClientWrapper = taishanClientWrapper;
		this.offHeapKeyPre = taishanClientWrapper.getOffHeapKeyPre();
	}

	@Override
	public void add(int keyGroupId, byte[] rawKeyBytes, int ttlTime) {
		cacheOffHeap.put(keyGroupId, new ByteArrayWrapper(cacheKeyWrapper(rawKeyBytes, offHeapKeyPre, true)), ttlTime, ttlTime);
	}

	@Override
	public void delete(int keyGroupId, byte[] rawKeyBytes) {
		cacheOffHeap.remove(keyGroupId, new ByteArrayWrapper(cacheKeyWrapper(rawKeyBytes, offHeapKeyPre, true)));
	}

	@Override
	public void snapshotFilter() {
		throw new FlinkRuntimeException("OffHeapBloomFilter not supported snapshotFliter.");
	}

	@Override
	public void restore() {
		throw new FlinkRuntimeException("OffHeapBloomFilter not supported snapshotFliter.(TaishanNormalRestoreOperation#restoreAllKeys)");
	}

	@Override
	public boolean mightContains(int keyGroupId, byte[] rawKeyBytes) {
		if (!checkAvailable()) {
			return true;
		}
		Integer ttlTime = cacheOffHeap.get(keyGroupId, new ByteArrayWrapper(cacheKeyWrapper(rawKeyBytes, offHeapKeyPre, true)));
		boolean contains = ttlTime != null && ttlTime > System.currentTimeMillis() / 1000;
		if (!contains) {
			taishanMetric.takeBloomFilterNull();
		}
		return contains;
	}

	@Override
	public void initTaishanMetric(TaishanMetric taishanMetric) {
		this.taishanMetric = taishanMetric;
	}

	public void isRestore() {
		this.isRestore = true;
		this.isAvailable = false;
	}

	public void setRestoreEarliestAvailableTime(long t) {
		this.restoreEarliestAvailableTime = t;
	}

	private boolean checkAvailable() {
		if (!isAvailable && System.currentTimeMillis() > restoreEarliestAvailableTime) {
			isAvailable = true;
		}
		return isAvailable;
	}
}
