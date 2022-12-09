package com.bilibili.bsql.common.metrics;

/** LookupMetrics. */
public interface LookupMetrics {
    /** Record rpc callback success. */
    void tps();

    /** Record rpc callback of failure. */
    void rpcCallbackFailure();

    /** Record rpc callback of failure. */
    void rpcCallbackFailure(int errorCode);

    /** Record the number of join success. */
    void sideJoinSuccess();

    /**
     * rtQuerySide.
     *
     * @param startTime
     */
    void rtQuerySide(long startTime);

    /** Record rpc timeout. */
    void rpcTimeout();

    /** Record rpc retryTimes. */
    void rpcRetryTimes();
}
