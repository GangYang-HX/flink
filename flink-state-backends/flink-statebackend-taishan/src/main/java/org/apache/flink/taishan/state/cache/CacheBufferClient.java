package org.apache.flink.taishan.state.cache;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.client.TaishanClientBatchWriteWrapper;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.taishan.state.cache.CacheEntry.*;
import static org.apache.flink.taishan.state.cache.OffHeapUtils.cacheKeyWrapper;

public class CacheBufferClient implements SnapshotableCacheClient {
	private static final Logger LOG = LoggerFactory.getLogger(CacheBufferClient.class);

	protected transient ExecutorService syncBackendExecutor;
	private final TCache<CacheEntry> cache;
	private final boolean isOffHeap;
	private final byte[] offHeapKeyPre;
	private final TaishanMetric taishanMetric;
	private final BlockingQueue<Tuple2<ByteArrayWrapper, CacheEntry>> cacheEntryQueue;
	private final List<TaishanClientBatchWriteWrapper> batchWriteWrapperList;
	private final TaishanClientWrapper taishanClientWrapper;
	private final AtomicBoolean hasBatchWritesFailures;

	public CacheBufferClient(TaishanClientWrapper taishanClientWrapper,
							 CacheConfiguration cacheConfiguration,
							 KeyGroupRange keyGroupRange,
							 TCache<CacheEntry> cache,
							 byte[] offHeapKeyPre,
							 TaishanMetric taishanMetric) {
		this.cache = cache;
		this.taishanMetric = taishanMetric;
		this.cacheEntryQueue = new LinkedBlockingQueue<>(cacheConfiguration.getCacheBlockingQueueSize());
		this.taishanMetric.getMetricGroup().gauge("blockQueueSize", () -> cacheEntryQueue.size());
		this.batchWriteWrapperList = new ArrayList<>();
		for (int i = keyGroupRange.getStartKeyGroup(); i <= keyGroupRange.getEndKeyGroup(); i++) {
			batchWriteWrapperList.add(new TaishanClientBatchWriteWrapper(taishanClientWrapper, taishanMetric));
		}
		this.taishanClientWrapper = taishanClientWrapper;
		this.hasBatchWritesFailures = new AtomicBoolean(false);
		this.syncBackendExecutor = Executors.newSingleThreadExecutor();
		this.isOffHeap = cacheConfiguration.isOffHeap();
		this.offHeapKeyPre = offHeapKeyPre;
		initAsyncConsumer(cacheConfiguration, keyGroupRange);
	}

	private void initAsyncConsumer(CacheConfiguration cacheConfiguration, KeyGroupRange keyGroupRange) {
		CacheEntryConsumer consumer = new CacheEntryConsumer(this.cacheEntryQueue,
			this.batchWriteWrapperList,
			cacheConfiguration.getCacheBatchWriteSize(),
			keyGroupRange,
			cacheConfiguration.getCacheBatchWriteInterval(),
			this.hasBatchWritesFailures,
			cacheConfiguration.getDynamicCache(),
			taishanMetric);
		this.syncBackendExecutor.submit(consumer);
	}

	@Override
	public void delete(int keyGroupId, byte[] rangeKey) {
		preConditionCheck();
		CacheEntry deleteEntry = new CacheEntry(keyGroupId, null, System.currentTimeMillis(), DELETE_ACTION, 0);
		cache.put(keyGroupId, new ByteArrayWrapper(cacheKeyWrapper(rangeKey, offHeapKeyPre, isOffHeap)), deleteEntry, 0);
		try {
			cacheEntryQueue.put(Tuple2.of(new ByteArrayWrapper(rangeKey), deleteEntry));
		} catch (InterruptedException e) {
		}
	}

	@Override
	public byte[] get(int keyGroupId, byte[] rangeKey) {
		preConditionCheck();
		byte[] valueBytes;
		CacheEntry cacheEntry = cache.get(keyGroupId, new ByteArrayWrapper(cacheKeyWrapper(rangeKey, offHeapKeyPre, isOffHeap)));

		if (cacheEntry != null) {
			valueBytes = cacheEntry.getDataObject();
			taishanMetric.takeCacheReadMetrics(valueBytes);
			// The data is already found in memory, whether it is null or not
			return valueBytes;
		}

		long start = taishanMetric.updateCatchingRate();
		valueBytes = taishanClientWrapper.get(keyGroupId, rangeKey);
		//Only record backend read metrics that are missing in cache.
		taishanMetric.takeReadMetrics(start, rangeKey, valueBytes);
		load(keyGroupId, rangeKey, valueBytes);

		return valueBytes;
	}

	@Override
	public void update(int keyGroupId, byte[] rangeKey, byte[] valueBytes, int ttlTime) {
		preConditionCheck();
		ByteArrayWrapper rawByteArray = new ByteArrayWrapper(rangeKey);
		CacheEntry cacheEntry = new CacheEntry(keyGroupId, valueBytes, System.currentTimeMillis(), WRITE_ACTION, ttlTime);
		cache.put(keyGroupId, new ByteArrayWrapper(cacheKeyWrapper(rangeKey, offHeapKeyPre, isOffHeap)), cacheEntry, ttlTime);
		try {
			cacheEntryQueue.put(Tuple2.of(rawByteArray, cacheEntry));
		} catch (InterruptedException e) {
		}
	}

	@Override
	public void removeAll(int keyGroupId, Iterable<ByteArrayWrapper> iterable) {
		cache.removeAll(keyGroupId, iterable);
	}

	private void load(int keyGroupIndex, byte[] rangeKey, byte[] valueBytes) {
		if (valueBytes != null) {
			CacheEntry cacheEntry = new CacheEntry(keyGroupIndex, valueBytes, System.currentTimeMillis(), DONE_ACTION, 0);
			cache.put(keyGroupIndex, new ByteArrayWrapper(cacheKeyWrapper(rangeKey, offHeapKeyPre, isOffHeap)), cacheEntry, 0);
		}
	}

	@Override
	public void flushBatchWrites() {
		preConditionCheck();
		CountDownLatch countDownLatch = new CountDownLatch(1);
		CacheEntry entry = new CacheEntryWatermark(-1, null, System.currentTimeMillis(), FLUSH_ACTION, countDownLatch);
		this.cacheEntryQueue.add(Tuple2.of(new ByteArrayWrapper(new byte[0]), entry));
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			throw new FlinkRuntimeException("Error happened when flushing batch writes", e);
		}
		// double check, preventing snapshot exceptions
		preConditionCheck();
	}

	@Override
	public void flushBatchWrites(int keyGroupId) {
		CountDownLatch countDownLatch = new CountDownLatch(1);
		CacheEntry entry = new CacheEntryWatermark(keyGroupId, null, System.currentTimeMillis(), FLUSH_ACTION, countDownLatch);
		this.cacheEntryQueue.add(Tuple2.of(new ByteArrayWrapper(new byte[0]), entry));
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			throw new FlinkRuntimeException("Error happened when flushing batch writes", e);
		}
	}

	@Override
	public void close() {
		if (this.syncBackendExecutor != null) {
			syncBackendExecutor.shutdown();
		}
	}

	@Override
	public boolean isEmpty() {
		return cache.isEmpty();
	}

	@Override
	public TaishanClientWrapper getTaishanClientWrapper() {
		return taishanClientWrapper;
	}

	private void preConditionCheck() {
		if (this.hasBatchWritesFailures.get()) {
			throw new FlinkRuntimeException("Batch Writes have errors. Throw runtime exception to stop main thread actions");
		}
	}

}
