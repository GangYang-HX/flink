package org.apache.flink.taishan.state.client;

import com.bilibili.taishan.model.Record;
import io.grpc.CallOptions;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.cache.ByteArrayWrapper;
import org.apache.flink.taishan.state.cache.CacheEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Dove
 * @Date 2022/7/7 2:32 下午
 */
public class TaishanClientBatchWriteWrapper extends TaishanClientBatchBaseWrapper {
	private final Map<ByteArrayWrapper, Record> recordBatch;

	private final List<CacheEntry> cacheEntries;

	private final List<CacheEntry> deleteCacheEntries;

	private final Set<ByteArrayWrapper> deleteBatch;
	private long currentBatchSize;

	public TaishanClientBatchWriteWrapper(TaishanClientWrapper taishanClientWrapper, TaishanMetric taishanMetric) {
		super(taishanClientWrapper, taishanMetric);
		currentBatchSize = 0;
		currentKeyGroupId = -1;
		recordBatch = new HashMap<>();
		deleteBatch = new HashSet<>();
		cacheEntries = new ArrayList<>();
		deleteCacheEntries = new ArrayList<>();
	}

	public void put(int keyGroupId,
					ByteArrayWrapper key,
					CacheEntry value) {

		if (currentKeyGroupId != keyGroupId) {
			flush();
		}
		deleteBatch.remove(key);
		currentKeyGroupId = keyGroupId;
		Record record = Record.builder().key(key.array()).value(value.getDataObject()).ttl(value.getTtlTime()).build();
		recordBatch.put(key, record);
		cacheEntries.add(value);

		currentBatchSize += key.array().length;
		currentBatchSize += value.getDataObject().length;
		currentBatchSize += 4;

		flushIfNeeded();
	}

	public void remove(int keyGroupId,
					   ByteArrayWrapper key,
					   CacheEntry entry) {
		if (currentKeyGroupId != keyGroupId) {
			flush();
		}
		recordBatch.remove(key);
		currentKeyGroupId = keyGroupId;
		deleteBatch.add(key);
		deleteCacheEntries.add(entry);

		flushIfNeeded();
	}

	private void flushIfNeeded() {
		boolean needFlush = (recordBatch.size() + deleteBatch.size() >= capacity) || (batchSize > 0 && currentBatchSize >= batchSize);
		if (needFlush) {
			flush();
		}
	}

	@Override
	public void flush() {
		if (recordBatch.size() == 0 && deleteBatch.size() == 0) {
			return;
		}
		//long start = taishanMetric.updateCatchingRate();
		if (recordBatch.size() != 0) {
			long start = System.nanoTime();
			taishanClientWrapper.batchPutOnShard(CallOptions.DEFAULT, currentKeyGroupId, new ArrayList<>(recordBatch.values()));
			taishanMetric.takeWriteBatchMetrics(start, recordBatch);
		}
		if (deleteBatch.size() != 0) {
			long start = System.nanoTime();
			taishanClientWrapper.batchDelOnShard(CallOptions.DEFAULT, currentKeyGroupId, deleteBatch.stream().map(ByteArrayWrapper::array).collect(Collectors.toList()));
			taishanMetric.takeDeleteBatchMetrics(start, deleteBatch);
		}
		lastFlushTime = System.currentTimeMillis();

		clearAll();
	}

	public int getCurrentRecordSize() {
		return recordBatch.size();
	}

	public int getCurrentDeleteRecordSize() {
		return deleteBatch.size();
	}

	public void clearAll() {
		recordBatch.clear();
		currentBatchSize = 0;
		for (CacheEntry cacheEntry : cacheEntries) {
			cacheEntry.setAction(CacheEntry.DONE_ACTION);
		}
		cacheEntries.clear();

		deleteBatch.clear();
		for (CacheEntry cacheEntry : deleteCacheEntries) {
			cacheEntry.setAction(CacheEntry.DONE_ACTION);
		}
		deleteCacheEntries.clear();
	}
}
