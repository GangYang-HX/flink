package com.bilibili.bsql.common.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;

/**
 * A wrapper to wrap sink table metric.
 */
public class SinkMetricsWrapper implements SinkMetrics {

    Counter successfulWrites;
    Counter failedWrites;
    Counter successfulSize;
    Histogram writeRt;
	Histogram flushRt;
	Counter retryNumber;
    private final Object lock = new Object();

    public SinkMetricsWrapper setSuccessfulWrites(Counter successfulWrites) {
        this.successfulWrites = successfulWrites;
        return this;
    }

    public SinkMetricsWrapper setFailedWrites(Counter failedWrites) {
        this.failedWrites = failedWrites;
        return this;
    }

    public SinkMetricsWrapper setWriteRt(Histogram writeRt) {
        this.writeRt = writeRt;
        return this;
    }

	public SinkMetricsWrapper setFlushRt(Histogram flushRt) {
		this.flushRt = flushRt;
		return this;
	}

	public SinkMetricsWrapper setSuccessfulSize(Counter successfulSize) {
		this.successfulSize = successfulSize;
		return this;
	}

	public SinkMetricsWrapper setRetryNumber(Counter retryNumber) {
		this.retryNumber = retryNumber;
		return this;
	}

    @Override
    public void writeSuccessRecord() {
        synchronized (lock) {
            successfulWrites.inc();
        }
    }

    @Override
    public void writeFailureRecord() {
        synchronized (lock) {
            failedWrites.inc();
        }
    }

	@Override
	public void writeSuccessSize(long size) {
		synchronized (lock) {
			successfulSize.inc(size);
		}
	}

	@Override
    public void rtWriteRecord(long start) {
        synchronized (lock) {
            writeRt.update(System.nanoTime() - start);
        }
    }

	@Override
	public void rtFlushRecord(long start) {
		synchronized (lock) {
			flushRt.update(System.nanoTime() - start);
		}
	}

	@Override
	public void retryRecord() {
		synchronized (lock) {
			retryNumber.inc();
		}

	}
}
