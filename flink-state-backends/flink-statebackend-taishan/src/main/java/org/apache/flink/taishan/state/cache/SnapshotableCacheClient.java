package org.apache.flink.taishan.state.cache;

import org.apache.flink.taishan.state.client.TaishanClientWrapper;

public interface SnapshotableCacheClient {
	void delete(int keyGroupId, byte[] rangeKey);

	byte[] get(int keyGroupId, byte[] rangeKey);

	void update(int keyGroupId, byte[] rangeKey, byte[] valueBytes, int ttlTime);

	void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable);

	boolean isEmpty();

	void close();

	void flushBatchWrites();

	void flushBatchWrites(int keyGroupId);

	TaishanClientWrapper getTaishanClientWrapper();
}
