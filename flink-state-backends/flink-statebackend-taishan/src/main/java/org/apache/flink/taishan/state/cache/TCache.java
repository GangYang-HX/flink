package org.apache.flink.taishan.state.cache;

/**
 * @author Dove
 * @Date 2022/10/10 8:14 下午
 */
public interface TCache<T> {
	void put(int keyGroupId, ByteArrayWrapper byteArrayWrapper, T cacheEntry, int ttlTime);

	T get(int keyGroupId, ByteArrayWrapper byteArrayWrapper);

	void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable);

	boolean isEmpty();

	void close();

	enum HeapType {
		HEAP,
		OFFHEAP,
		NONE
	}
}
