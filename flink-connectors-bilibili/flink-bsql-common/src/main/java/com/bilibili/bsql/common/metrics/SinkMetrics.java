package com.bilibili.bsql.common.metrics;

/**
 * User defined metrics should implement this interface.
 */
public interface SinkMetrics extends Metrics {

    /**
     * Number of success records written to sink.
     */
    void writeSuccessRecord();

    /**
     * Number of failed records written to sink.
     */
    void writeFailureRecord();

    /**
     * Record write time.
     */
    void rtWriteRecord(long start);

	/**
	 * Record flush time
	 */
	void rtFlushRecord(long start);

	/**
	 * Record retry numbers
	 */
	void retryRecord();

	/**
	 * Number of success records size.
	 */
	void writeSuccessSize(long size);

}
