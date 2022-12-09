package org.apache.flink.taishan.state.cache;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Dove
 * @Date 2022/10/10 2:40 下午
 */
public class TCacheOffHeap<T> implements TCache<T> {
	private static final Logger LOG = LoggerFactory.getLogger(TCacheOffHeap.class);

	private final OHCache<ByteArrayWrapper, T> cache;
	private final long capacityBytes;
	private final boolean offHeapTtlEnable;
	//	private final int cacheBatchWriteInterval;
	private Eviction eviction = Eviction.LRU;

	public TCacheOffHeap(CacheSerializer<T> valueSerializer,
						 long capacityBytes,
						 int segmentCount,
						 boolean offHeapTtlEnable) {
		this.capacityBytes = capacityBytes;
		this.offHeapTtlEnable = offHeapTtlEnable;
		this.cache = this.builder(valueSerializer)
			.capacity(capacityBytes)
			.segmentCount(segmentCount)
			.timeouts(offHeapTtlEnable)
			.build();
	}

	private OHCacheBuilder<ByteArrayWrapper, T> builder(CacheSerializer<T> valueSerializer) {
		return OHCacheBuilder.<ByteArrayWrapper, T>newBuilder()
			.keySerializer(new KeySerializer())
			.valueSerializer(valueSerializer)
			.eviction(eviction)
			.timeoutsSlots(1)
			.throwOOME(true);
	}

	@Override
	public void put(int keyGroupId, ByteArrayWrapper byteArrayWrapper, T cacheEntry, int ttlTime) {
		long ttl = offHeapTtlEnable ? ttlTime * 1000L : 0L;
		if (!cache.put(byteArrayWrapper, cacheEntry, ttl)) {
			throw new FlinkRuntimeException("OffHeap put entry failed.");
		}
	}

	public void remove(int keyGroupId, ByteArrayWrapper byteArrayWrapper) {
		cache.remove(byteArrayWrapper);
	}

	@Override
	public T get(int keyGroupId, ByteArrayWrapper byteArrayWrapper) {
		return cache.get(byteArrayWrapper);
	}

	@Override
	public void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable) {
		cache.removeAll(iterable);
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public void close() {
		try {
			cache.close();
		} catch (IOException e) {
			throw new FlinkRuntimeException("offheap close failed.", e);
		}
	}

	public void setDataTtl(long ttl) {
		if (eviction == Eviction.LRU) {
			// only Eviction.LRU supported ttl and timeouts
			cache.setDataTtl(ttl);
		}
	}

	public long offHeapCapacityBytes() {
		return capacityBytes;
	}

	public long memUsed() {
		return cache.memUsed();
	}

	public long freeCapacity() {
		return cache.freeCapacity();
	}

	public long capacity() {
		return cache.capacity();
	}

	public long size() {
		return cache.size();
	}

	public long hitCount() {
		return cache.hitCount();
	}

	public long missCount() {
		return cache.missCount();
	}

	public long evictedEntries() {
		return cache.evictedEntries();
	}

	public long putAddCount() {
		return cache.putAddCount();
	}

	public long expiredEntries() {
		return cache.expiredEntries();
	}

	public long rehashes() {
		return cache.rehashes();
	}

	public long getTimeOutsCompactionNum() {
		return cache.getTimeOutsCompactionNum();
	}

	public long getMaybeCompactNum() {
		return cache.getMaybeCompactNum();
	}

	private class KeySerializer implements CacheSerializer<ByteArrayWrapper> {

		@Override
		public ByteArrayWrapper deserialize(ByteBuffer input) {
			byte[] bytes = new byte[input.getInt()];
			input.get(bytes);
			return new ByteArrayWrapper(bytes);
		}

		@Override
		public void serialize(ByteArrayWrapper key, ByteBuffer output) {
			output.putInt(key.array().length);
			output.put(key.array());
		}

		@Override
		public int serializedSize(ByteArrayWrapper key) {
			// NOTE: return size must be == actual bytes to write
			return 4 + key.array().length;
		}
	}

	public static class DataValueSerializer implements CacheSerializer<CacheEntry> {
		private final KryoSerializer<CacheEntry> valueSerializer;
		private final DataOutputSerializer dataOutputView;
		private final DataInputDeserializer dataInputView;
		private final int cacheBatchWriteInterval;

		public DataValueSerializer(KryoSerializer<CacheEntry> valueSerializer, int cacheBatchWriteInterval) {
			this.valueSerializer = valueSerializer;
			this.dataOutputView = new DataOutputSerializer(128);
			this.dataInputView = new DataInputDeserializer();
			this.cacheBatchWriteInterval = cacheBatchWriteInterval;
		}

		@Override
		public synchronized CacheEntry deserialize(ByteBuffer input) {
			try {
				dataInputView.setBuffer(input);
				return valueSerializer.deserialize(dataInputView);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Deserialize CacheEntry error.");
			}
		}

		@Override
		public synchronized void serialize(CacheEntry value, ByteBuffer output) {
			// int keyGroupIndex, byte[] data, long timeStamp, byte action,int ttlTime
			dataOutputView.clear();
			try {
				valueSerializer.serialize(value, dataOutputView);
				output.put(dataOutputView.getCopyOfBuffer());
			} catch (IOException e) {
				throw new FlinkRuntimeException("Serialize CacheEntry error:{}" + value.toString());
			}
		}

		@Override
		public synchronized int serializedSize(CacheEntry value) {
			// NOTE: return size must be == actual bytes to write
			dataOutputView.clear();
			try {
				valueSerializer.serialize(value, dataOutputView);
				return dataOutputView.getCopyOfBuffer().length;
			} catch (IOException e) {
				throw new FlinkRuntimeException("Serialize CacheEntry error:{}" + value.toString());
			}
		}

		@Override
		public boolean elementCouldRemove(long hashEntryAdr, Element e) {
			CacheEntry deserialize = deserialize(e.getByteBuffer(hashEntryAdr));
			boolean couldRemove = deserialize.getTimeStamp() + cacheBatchWriteInterval < System.currentTimeMillis();
			if (!couldRemove) {
				throw new OutOfMemoryError("Unable to removeEldest in (Key-Value)off-heap, try increasing the out-of-heap memory.");
			}
			return true;
		}
	}

	public static class VIntSerializer implements CacheSerializer<Integer> {
		private final IntSerializer intSerializer;
		private final DataOutputSerializer dataOutputView;
		private final DataInputDeserializer dataInputView;
		private final int cacheBatchWriteInterval;

		public VIntSerializer(IntSerializer intSerializer, int cacheBatchWriteInterval) {
			this.intSerializer = intSerializer;
			this.dataOutputView = new DataOutputSerializer(128);
			this.dataInputView = new DataInputDeserializer();
			this.cacheBatchWriteInterval = cacheBatchWriteInterval;
		}

		@Override
		public synchronized Integer deserialize(ByteBuffer input) {
			try {
				dataInputView.setBuffer(input);
				return intSerializer.deserialize(dataInputView);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Deserialize Long error.");
			}
		}

		@Override
		public synchronized void serialize(Integer value, ByteBuffer output) {
			dataOutputView.clear();
			try {
				intSerializer.serialize(value, dataOutputView);
				output.put(dataOutputView.getCopyOfBuffer());
			} catch (IOException e) {
				throw new FlinkRuntimeException("Serialize Long error:{}" + value.toString());
			}
		}

		@Override
		public synchronized int serializedSize(Integer value) {
			// NOTE: return size must be == actual bytes to write
			dataOutputView.clear();
			try {
				intSerializer.serialize(value, dataOutputView);
				return dataOutputView.getCopyOfBuffer().length;
			} catch (IOException e) {
				throw new FlinkRuntimeException("Serialize Long error:{}" + value.toString());
			}
		}

		@Override
		public boolean elementCouldRemove(long hashEntryAdr, Element e) {
			Integer ttlSeconds = deserialize(e.getByteBuffer(hashEntryAdr));
			boolean couldRemove = ttlSeconds * 1000L + cacheBatchWriteInterval < System.currentTimeMillis();
			if (!couldRemove) {
				throw new OutOfMemoryError("Unable to removeEldest in (All Keys)off-heap, try increasing the out-of-heap memory.");
			}
			return true;
		}
	}
}
