package com.bilibili.bsql.common.metrics;

/**
 * User defined metrics should implement this interface.
 */
public interface SideMetrics extends Metrics {

    /**
     * Record rpc callback success.
     */
    void tps();

    /**
     * Record rpc callback of failure.
     */
    void rpcCallbackFailure();

	/**
	 * Record rpc callback of failure.
	 */
	void rpcCallbackFailure(int errorCode);

    /**
     * Record the number of join success.
     */
    void sideJoinSuccess();

    /**
     * Record the number of join fail.
     */
    void sideJoinMiss();

    /**
     * Record the number of join encounter exception.
     */
    void sideJoinException();

    /**
     * rtQuerySide
     * @param startTime
     */
    void rtQuerySide(long startTime);

    /**
     * Record rpc timeout.
     */
    void rpcTimeout();

	/**
	 * Record rpc retryTimes.
	 */
    void rpcRetryTimes();
}
