package org.apache.flink.taishan.state.cache;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.client.TaishanClientBatchWriteWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.taishan.state.cache.CacheEntry.FLUSH_ACTION;
import static org.apache.flink.taishan.state.cache.CacheEntry.WRITE_ACTION;

/**
 * @author Dove
 * @Date 2022/10/10 5:26 下午
 */
public class CacheEntryConsumer implements Runnable {
	private final static Logger LOG = LoggerFactory.getLogger(CacheEntryConsumer.class);
	BlockingQueue<Tuple2<ByteArrayWrapper, CacheEntry>> cacheEntryQueue;
	private static final int REFRESH_FLUSH_COUNTER_DURATION_COUNT = 20;

	int startKeyGroup;

	int endKeyGroup;

	long maxFlushDuration;

	private final int maxBatchCount;
	private final int topBatchCount;
	private final int lowestBatchCount;
	private final boolean dynamicCache;
	List<TaishanClientBatchWriteWrapper> batchWriteWrapperList;
	final AtomicBoolean hasBatchWritesFailures;

	private final List<Integer> writeBatches = new ArrayList<>();
	private final List<Integer> flushDurations = new ArrayList<>();
	private final Map<Integer, Integer> pastFlushCountStats = new HashMap<>();
	private final Map<Integer, Long> pastFlushTimeStats = new HashMap<>();
	private final Map<Integer, Integer> statsCountMap = new HashMap<>();
	private final TaishanMetric taishanMetric;

	public CacheEntryConsumer(BlockingQueue<Tuple2<ByteArrayWrapper, CacheEntry>> cacheEntryQueue,
							  List<TaishanClientBatchWriteWrapper> batchWriteWrapperList,
							  int batchCount,
							  KeyGroupRange keyGroupRange,
							  long maxFlushDuration,
							  AtomicBoolean hasBatchWritesFailures,
							  boolean dynamicCache,
							  TaishanMetric taishanMetric) {
		this.cacheEntryQueue = cacheEntryQueue;
		this.batchWriteWrapperList = batchWriteWrapperList;
		this.maxBatchCount = batchCount;
		this.topBatchCount = (int) ((double) maxBatchCount * 1.5f);
		this.lowestBatchCount = (int) ((double) maxBatchCount * 0.5f);
		this.startKeyGroup = keyGroupRange.getStartKeyGroup();
		this.endKeyGroup = keyGroupRange.getEndKeyGroup();
		for (int i = startKeyGroup; i <= endKeyGroup; i++) {
			writeBatches.add((int) ((double) maxBatchCount * 1f));
			flushDurations.add((int) ((double) maxFlushDuration * 1f));
			statsCountMap.put(i, 0);
		}
		this.maxFlushDuration = maxFlushDuration;
		this.hasBatchWritesFailures = hasBatchWritesFailures;
		this.taishanMetric = taishanMetric;
		this.dynamicCache = dynamicCache;
		LOG.info("CacheEntryConsumer, maxBatchCount: {}, maxFlushDuration:{}.", this.maxBatchCount, this.maxFlushDuration);
	}


	public void run() {
		while (true) {
			long currentTime = System.currentTimeMillis();
			taishanMetric.updateBatchCatchingRate();
			try {
				Tuple2<ByteArrayWrapper, CacheEntry> entry = cacheEntryQueue.take();
				int keyGroupIndex = entry.f1.getKeyGroupIndex();
				int keyGroupIndexDist = keyGroupIndex - startKeyGroup;
				try {
					if (entry.f1 != null && entry.f1.getAction() == FLUSH_ACTION) {
						if (keyGroupIndex == -1) {
							// flush all
							for (int i = startKeyGroup; i <= endKeyGroup; i++) {
								this.batchWriteWrapperList.get(i - startKeyGroup).flush();
							}
						} else {
							// flush current keyGroup data
							this.batchWriteWrapperList.get(keyGroupIndexDist).flush();
						}
						CacheEntryWatermark cacheEntryWatermark = (CacheEntryWatermark) entry.f1;
						cacheEntryWatermark.getLatch().countDown();
					} else if (entry.f1 != null && entry.f1.getTimeStamp() > 0) {
						TaishanClientBatchWriteWrapper writeWrapper = this.batchWriteWrapperList.get(keyGroupIndexDist);
						if (entry.f1.getDataObject() != null && entry.f1.getAction() == WRITE_ACTION) {
							writeWrapper.put(keyGroupIndex, entry.f0, entry.f1);
						} else if (entry.f1 != null && entry.f1.getAction() == CacheEntry.DELETE_ACTION) {
							writeWrapper.remove(keyGroupIndex, entry.f0, entry.f1);
						}

						int numberInQueue = writeWrapper.getCurrentDeleteRecordSize() + writeWrapper.getCurrentRecordSize();
						long lastFlushTime = writeWrapper.getLastFlushTime();
						int currentBatchCount = this.writeBatches.get(keyGroupIndexDist);
						int currentFlushDuration = this.flushDurations.get(keyGroupIndexDist);
						long currentWaitTime = currentTime - lastFlushTime;
						if (numberInQueue > currentBatchCount || currentWaitTime > currentFlushDuration) {
							writeWrapper.flush();
							dynamicAccCountAndTimes(keyGroupIndex, numberInQueue, currentWaitTime);
						}

					}
				} catch (Exception e) {
					LOG.error("Error in writing to taishan in batch", e);
					hasBatchWritesFailures.getAndSet(true);
					notifyWatermarkCompleted(entry);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			taishanMetric.batchLoopLock++;
		}
	}

	private void dynamicAccCountAndTimes(int keyGroupIndex, int numberInQueue, long currentWaitTime) {
		if (!dynamicCache) {
			return;
		}
		int keyGroupIndexDist = keyGroupIndex - startKeyGroup;
		int currentBatchCount = this.writeBatches.get(keyGroupIndexDist);
		int currentFlushDuration = this.flushDurations.get(keyGroupIndexDist);

		int statsCount = statsCountMap.get(keyGroupIndex);
		int statsCountModel = statsCount % REFRESH_FLUSH_COUNTER_DURATION_COUNT;
		// update the stats
		if (statsCountModel == 0) {
			pastFlushCountStats.put(keyGroupIndex, numberInQueue);
			pastFlushTimeStats.put(keyGroupIndex, currentWaitTime);
		} else {
			Integer pastAvgCount = pastFlushCountStats.get(keyGroupIndex);
			pastAvgCount = (pastAvgCount * statsCountModel + numberInQueue) / (statsCountModel + 1);
			Long pastAvgFlushTime = pastFlushTimeStats.get(keyGroupIndex);
			pastAvgFlushTime = (pastAvgFlushTime * statsCountModel + currentWaitTime) / (statsCountModel + 1);
			pastFlushCountStats.put(keyGroupIndex, pastAvgCount);
			pastFlushTimeStats.put(keyGroupIndex, pastAvgFlushTime);
		}

		if (statsCount > 0 && statsCountModel == 0) {
			Integer pastAvgCount = pastFlushCountStats.get(keyGroupIndex);

			// past flushed count is always close to current level. Increase the threshold.
			if (pastAvgCount >= 0.95 * currentBatchCount) {
				int newBatchCount = (int) ((double) pastAvgCount * 1.05f);
				newBatchCount = (int) ((double) newBatchCount * 1.05f);
				newBatchCount = newBatchCount > topBatchCount ? topBatchCount : newBatchCount;
				this.writeBatches.set(keyGroupIndexDist, newBatchCount);
			} else if (pastAvgCount > numberInQueue * 20) {
				int newBatchCount = (int) ((double) pastAvgCount * 0.95f);
				newBatchCount = (int) ((double) newBatchCount * 0.95f);
				newBatchCount = newBatchCount < lowestBatchCount ? lowestBatchCount : newBatchCount;
				this.writeBatches.set(keyGroupIndexDist, newBatchCount);
			} else if (pastAvgCount <= 0.2 * maxBatchCount) {
				// This means we have reached the wait time but only has a few records to flush.
				// Increase the wait time.
				int newFlushDuration = (int) ((double) currentFlushDuration * 1.1f);
				this.flushDurations.set(keyGroupIndexDist, newFlushDuration);
			}
		}
		statsCountMap.put(keyGroupIndex, ++statsCount);
	}

	private void notifyWatermarkCompleted(Tuple2<ByteArrayWrapper, CacheEntry> entry) {
		if (entry.f1 != null && entry.f1.getAction() == FLUSH_ACTION) {
			CacheEntryWatermark cacheEntryWatermark = (CacheEntryWatermark) entry.f1;
			cacheEntryWatermark.getLatch().countDown();
		}
	}
}
