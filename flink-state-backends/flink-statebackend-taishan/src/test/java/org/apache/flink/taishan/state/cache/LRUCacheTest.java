package org.apache.flink.taishan.state.cache;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class LRUCacheTest extends TestLogger {
	@Test
	public void testLRUCacheSize() throws Exception {
		LRUCache lruCache = new LRUCache(10, 10 * 1000);
		assertEquals(0, lruCache.size());
		assertEquals(0, lruCache.list.linkedListSize());
		byte[] rawKeyBytes1 = new byte[]{0};
		byte[] rawDataBytes1 = new byte[]{0};
		CacheEntry cacheEntry1 = new CacheEntry(-1, rawDataBytes1, System.currentTimeMillis(), CacheEntry.WRITE_ACTION, 0);
		lruCache.put(new ByteArrayWrapper(rawKeyBytes1), cacheEntry1);
		assertEquals(1, lruCache.size());
		assertEquals(1, lruCache.list.linkedListSize());
		for (Integer i = 1; i <= 10; i++) {
			byte[] rawKeyBytes = new byte[]{i.byteValue()};
			CacheEntry cacheEntry = new CacheEntry(-1, rawKeyBytes, System.currentTimeMillis(), CacheEntry.DONE_ACTION, 0);
			lruCache.put(new ByteArrayWrapper(rawKeyBytes), cacheEntry);
		}

		for (Integer i = 1; i <= 10; i++) {
			byte[] rawKeyBytes = new byte[]{i.byteValue()};
			CacheEntry cacheEntry = lruCache.get(new ByteArrayWrapper(rawKeyBytes));
			assertNotNull(cacheEntry);
			assertTrue(Arrays.equals(cacheEntry.getDataObject(), rawKeyBytes));
		}

		assertEquals(11, lruCache.size());
		assertEquals(11, lruCache.list.linkedListSize());
		Thread.sleep(10 * 1000);

		// After some time, the entries have been expired.
		for (Integer i = 1; i <= 10; i++) {
			byte[] rawKeyBytes = new byte[]{i.byteValue()};
			CacheEntry cacheEntry = lruCache.get(new ByteArrayWrapper(rawKeyBytes));
			assertNull(cacheEntry);
		}

		byte[] rawKeyBytes = new byte[]{11};
		byte[] rawValueBytes = new byte[]{11};
		CacheEntry cacheEntry = new CacheEntry(-1, rawValueBytes, System.currentTimeMillis(), CacheEntry.DONE_ACTION, 0);
		lruCache.put(new ByteArrayWrapper(rawKeyBytes), cacheEntry);

		// The item added to for loop has been deleted.
		// Only 2 left: One is write_action, means it has not been persisted. The other is just added and has not been deleted.
		assertEquals(2, lruCache.size());
		assertEquals(2, lruCache.list.linkedListSize());

		for (Integer i = 1; i <= 10; i++) {
			rawKeyBytes = new byte[]{i.byteValue()};
			cacheEntry = lruCache.get(new ByteArrayWrapper(rawKeyBytes));
			assertNull(cacheEntry);
		}
	}

	@Test
	public void testLRUCacheWithExpiredTime() throws Exception {
		LRUCache lruCache = new LRUCache(10, 10 * 1000);
		for (Integer i = 0; i < 10; i++) {
			byte[] rawKeyBytes = new byte[]{i.byteValue()};
			CacheEntry cacheEntry = new CacheEntry(-1, rawKeyBytes, System.currentTimeMillis(), CacheEntry.DONE_ACTION, 0);
			lruCache.put(new ByteArrayWrapper(rawKeyBytes), cacheEntry);
		}

		assertEquals(10, lruCache.size());
		assertEquals(10, lruCache.list.linkedListSize());

		Thread.sleep(10 * 1000);

		byte[] rawKeyBytes1 = new byte[]{11};
		CacheEntry cacheEntry1 = new CacheEntry(-1, rawKeyBytes1, System.currentTimeMillis(), CacheEntry.DONE_ACTION, 0);
		lruCache.put(new ByteArrayWrapper(rawKeyBytes1), cacheEntry1);

		assertEquals(1, lruCache.size());
		assertEquals(1, lruCache.list.linkedListSize());

		for (Integer i = 0; i < 10; i++) {
			byte[] rawKeyBytes = new byte[]{i.byteValue()};
			CacheEntry cacheEntry = new CacheEntry(-1, rawKeyBytes, System.currentTimeMillis(), CacheEntry.DONE_ACTION, 0);
			lruCache.put(new ByteArrayWrapper(rawKeyBytes), cacheEntry);
		}

		assertEquals(11, lruCache.size());
		assertEquals(11, lruCache.list.linkedListSize());

		CacheEntryTestConsumer consumer = new CacheEntryTestConsumer(lruCache);
		ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
		consumerExecutor.submit(consumer);
		Thread.sleep(10 * 1000);

		assertEquals(11, lruCache.size());
		assertEquals(11, lruCache.list.linkedListSize());

		lruCache.put(new ByteArrayWrapper(rawKeyBytes1), cacheEntry1);

		assertEquals(1, lruCache.size());
		assertEquals(1, lruCache.list.linkedListSize());
	}


	public class CacheEntryTestConsumer implements Runnable {
		LRUCache lruCache;
		CacheEntryTestConsumer(LRUCache lruCache) {
			this.lruCache = lruCache;
		}
		public void run() {
			for (Integer i = 0; i < 10; i++) {
				byte[] rawKeyBytes = new byte[]{i.byteValue()};
				lruCache.markAsPersisted(new ByteArrayWrapper(rawKeyBytes));
			}
		}
	}
}

