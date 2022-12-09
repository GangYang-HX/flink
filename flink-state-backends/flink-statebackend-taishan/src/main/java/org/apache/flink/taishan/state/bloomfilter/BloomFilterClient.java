package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.taishan.state.TaishanMetric;

/**
 * @author Dove
 * @Date 2022/9/8 11:53 上午
 */
public interface BloomFilterClient {

	/**
	 * @param ttlTime seconds
	 */
	void add(int keyGroupId, byte[] rawKeyBytes, int ttlTime);

	void delete(int keyGroupId, byte[] rawKeyBytes);

	void snapshotFilter();

	void restore();

	boolean mightContains(int keyGroupId, byte[] rawKeyBytes);

	void initTaishanMetric(TaishanMetric taishanMetric);
}
