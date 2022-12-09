package org.apache.flink.taishan.state.cache;

import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;

/**
 * @author Dove
 * @Date 2022/9/8 4:56 下午
 */
public class NoneCacheClient implements SnapshotableCacheClient {
	private final TaishanMetric taishanMetric;
	private final TaishanClientWrapper taishanClientWrapper;

	public NoneCacheClient(TaishanMetric taishanMetric, TaishanClientWrapper taishanClientWrapper) {
		this.taishanMetric = taishanMetric;
		this.taishanClientWrapper = taishanClientWrapper;
	}

	@Override
	public void delete(int keyGroupId, byte[] rangeKey) {
		long start = taishanMetric.updateCatchingRate();
		taishanClientWrapper.del(keyGroupId, rangeKey);
		taishanMetric.takeDeleteMetrics(start, rangeKey);
	}

	@Override
	public byte[] get(int keyGroupId, byte[] rangeKey) {
		long start = taishanMetric.updateCatchingRate();
		byte[] valueBytes = taishanClientWrapper.get(keyGroupId, rangeKey);
		taishanMetric.takeReadMetrics(start, rangeKey, valueBytes);
		return valueBytes;
	}

	@Override
	public void update(int keyGroupId, byte[] rangeKey, byte[] valueBytes, int ttlTime) {
		long start = taishanMetric.updateCatchingRate();
		taishanClientWrapper.put(keyGroupId, rangeKey, valueBytes, ttlTime);
		taishanMetric.takeWriteMetrics(start, rangeKey, valueBytes);
	}

	@Override
	public void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable) {

	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public void close() {

	}

	@Override
	public void flushBatchWrites() {

	}

	@Override
	public void flushBatchWrites(int keyGroupId) {

	}

	@Override
	public TaishanClientWrapper getTaishanClientWrapper() {
		return taishanClientWrapper;
	}
}
