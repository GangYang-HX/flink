package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.taishan.state.TaishanMetric;

/**
 * @author Dove
 * @Date 2022/9/8 3:32 下午
 */
public class TaishanNonBloomFilterClient implements BloomFilterClient {
	@Override
	public void add(int keyGroupId, byte[] rawKeyBytes, int ttlTime) {

	}

	@Override
	public void delete(int keyGroupId, byte[] rawKeyBytes) {

	}

	@Override
	public void snapshotFilter() {

	}

	@Override
	public void restore() {

	}

	@Override
	public boolean mightContains(int keyGroupId, byte[] rawKeyBytes) {
		return true;
	}

	@Override
	public void initTaishanMetric(TaishanMetric taishanMetric) {

	}
}
