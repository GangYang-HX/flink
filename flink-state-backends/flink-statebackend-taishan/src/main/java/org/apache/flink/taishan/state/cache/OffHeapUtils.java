package org.apache.flink.taishan.state.cache;

/**
 * @author Dove
 * @Date 2022/11/3 3:20 下午
 */
public class OffHeapUtils {
	public static int OFF_HEAP_KEY_PREFIX_LENGTH = 8;

	public static byte[] cacheKeyWrapper(byte[] k, byte[] offHeapKeyPre, boolean isOffHeap) {
		if (isOffHeap) {
			byte[] bt3 = new byte[k.length + offHeapKeyPre.length];
			System.arraycopy(offHeapKeyPre, 0, bt3, 0, offHeapKeyPre.length);
			System.arraycopy(k, 0, bt3, offHeapKeyPre.length, k.length);
			return bt3;
		} else {
			return k;
		}
	}

	public static byte[] cacheKeyStore(byte[] k) {
		byte[] b = new byte[k.length - OFF_HEAP_KEY_PREFIX_LENGTH];
		System.arraycopy(k, OFF_HEAP_KEY_PREFIX_LENGTH, b, 0, b.length);
		return b;
	}
}
