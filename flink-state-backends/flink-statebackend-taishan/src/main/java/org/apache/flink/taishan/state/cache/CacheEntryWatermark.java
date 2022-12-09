package org.apache.flink.taishan.state.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class CacheEntryWatermark extends CacheEntry {
	CountDownLatch latch;

	public CacheEntryWatermark(int keyGroupIndex, byte[] data, long timeStamp, byte action, CountDownLatch latch) {
		super(keyGroupIndex, data, timeStamp, action,0);
		this.latch = latch;
	}

	public CountDownLatch getLatch() {
		return latch;
	}
}
