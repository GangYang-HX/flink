package org.apache.flink.taishan.state.cache;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class CacheEntry implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final byte INVALID_ACTION = -1;

	public static final byte DONE_ACTION = 0;
	public static final byte WRITE_ACTION = 1;

	public static final byte DELETE_ACTION = 2;

	public static final byte MERGE_ACTION = 3;

	public static final byte FLUSH_ACTION = 4;

	byte[] dataObject;

	volatile long timeStamp;

	int keyGroupIndex;

	volatile byte action;

	private final int ttlTime;

	public byte[] getDataObject() {
		return dataObject;
	}

	public void setDataObject(byte[] dataObject) {
		this.dataObject = dataObject;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public byte getAction() {
		return action;
	}

	public void setAction(byte action) {
		this.action = action;
	}

	public int getKeyGroupIndex() {
		return keyGroupIndex;
	}

	public int getTtlTime() {
		return ttlTime;
	}

	public void setKeyGroupIndex(int keyGroupIndex) {
		this.keyGroupIndex = keyGroupIndex;
	}

	public static final CacheEntry EMPTY_CACHE_ENTRY = new CacheEntry(-1, new byte[0], 0, WRITE_ACTION, 0);
	public static final CacheEntry DELETE_CACHE_ENTRY = new CacheEntry(-1, new byte[0], 0, DELETE_ACTION, 0);

	public CacheEntry(int keyGroupIndex, byte[] data, long timeStamp, byte action,int ttlTime) {
		this.keyGroupIndex = keyGroupIndex;
		this.dataObject = data;
		this.timeStamp = timeStamp;
		this.action = action;
		this.ttlTime = ttlTime;
	}
}
