package org.apache.flink.taishan.state.bloomfilter;

/**
 * @author Dove
 * @Date 2022/9/8 2:22 下午
 */
public class BloomFilterUtils {
	private static final double LN2 = Math.log(2);
	private static final double LN2_SQUARED = LN2 * LN2;

	static int optimalNumOfBits(long n, double p) {
		return (int) (-n * Math.log(p) / LN2_SQUARED);
	}

	static int optimalNumOfHashFunctions(long n, long m) {
		// (m / n) * log(2), but avoid truncation due to division!
		return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
	}

}
