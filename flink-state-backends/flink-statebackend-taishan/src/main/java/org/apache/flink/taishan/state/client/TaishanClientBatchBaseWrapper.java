package org.apache.flink.taishan.state.client;

import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;

/**
 * @author Dove
 * @Date 2022/7/6 9:03 下午
 */
public abstract class TaishanClientBatchBaseWrapper implements AutoCloseable {

	private static final int MIN_CAPACITY = 100;
	private static final int MAX_CAPACITY = 1000;
	private static final int PER_RECORD_BYTES = 100;
	// default 0 for disable memory size based flush
	private static final long DEFAULT_BATCH_SIZE = 0;

	protected final TaishanClientWrapper taishanClientWrapper;
	protected final int capacity;
	@Nonnegative
	protected final long batchSize;
	protected final long maxBatchSize;
	protected final TaishanMetric taishanMetric;
	protected int currentKeyGroupId;

	protected long lastFlushTime;

	public TaishanClientBatchBaseWrapper(TaishanClientWrapper taishanClientWrapper, TaishanMetric taishanMetric) {
		this(taishanClientWrapper, 1000, DEFAULT_BATCH_SIZE, taishanMetric);
	}

	public TaishanClientBatchBaseWrapper(TaishanClientWrapper taishanClientWrapper,
										 int capacity,
										 long batchSize,
										 TaishanMetric taishanMetric) {
		Preconditions.checkArgument(capacity >= MIN_CAPACITY && capacity <= MAX_CAPACITY,
			"capacity should be between " + MIN_CAPACITY + " and " + MAX_CAPACITY);
		Preconditions.checkArgument(batchSize >= 0, "Max batch size have to be no negative.");

		this.taishanClientWrapper = taishanClientWrapper;
		this.capacity = capacity;
		this.batchSize = batchSize;
		this.taishanMetric = taishanMetric;
		if (this.batchSize > 0) {
			this.maxBatchSize = Math.min(this.batchSize, this.capacity * PER_RECORD_BYTES);
		} else {
			this.maxBatchSize = this.capacity * PER_RECORD_BYTES;
		}
		this.lastFlushTime = System.currentTimeMillis();
	}

	@Override
	public void close() {
		flush();
	}

	public void flush() {
	}


	public long getLastFlushTime() {
		return lastFlushTime;
	}
}
