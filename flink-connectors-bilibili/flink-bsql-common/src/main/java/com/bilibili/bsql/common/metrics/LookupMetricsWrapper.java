package com.bilibili.bsql.common.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.functions.FunctionContext;

import java.util.HashMap;
import java.util.Map;

/** LookupMetricsWrapper. */
public class LookupMetricsWrapper implements LookupMetrics {
    protected MetricGroup globalGroup;
    protected MetricGroup localGroup;
    protected String lookupTableType;

    private final Counter tps;
    private final Counter threadRpcCallbackFailure;
    private final Map<Integer, Counter> threadRpcCallbackFailureMap;
    private final Counter threadJoinSuccess;
    private final Counter threadRpcTimeout;
    private final Counter threadRetryTimes;
    private final Histogram threadRtQuery;

    public LookupMetricsWrapper(String lookupTableType, FunctionContext runtimeContext) {
        this.lookupTableType = lookupTableType;
        globalGroup = runtimeContext.getMetricGroup();

        localGroup = globalGroup.addGroup("thread_" + lookupTableType, QueryServiceMode.DISABLED);

        this.tps = localGroup.counter("tps");

        this.threadRpcCallbackFailure = localGroup.counter("rpcCallbackFailure");

        this.threadRpcCallbackFailureMap = new HashMap<>();

        this.threadJoinSuccess = localGroup.counter("joinSuccess");

        this.threadRpcTimeout = localGroup.counter("rpcTimeout");

        int windowSize = 10000;
        this.threadRtQuery =
                localGroup.histogram("rt", new DescriptiveStatisticsHistogram(windowSize));

        this.threadRetryTimes = localGroup.counter("retryTimes");
    }

    @Override
    public void tps() {
        synchronized (tps) {
            tps.inc();
        }
    }

    @Override
    public void rpcCallbackFailure() {
        synchronized (threadRpcCallbackFailure) {
            threadRpcCallbackFailure.inc();
        }
    }

    @Override
    public void rpcCallbackFailure(int errorCode) {
        synchronized (threadRpcCallbackFailure) {
            threadRpcCallbackFailureMap.putIfAbsent(
                    errorCode,
                    globalGroup
                            .addGroup(
                                    "rpc_error",
                                    String.valueOf(errorCode),
                                    QueryServiceMode.DISABLED)
                            .counter("rpcCallbackFailureMap"));
            threadRpcCallbackFailureMap.get(errorCode).inc();
        }
    }

    @Override
    public void sideJoinSuccess() {
        synchronized (threadJoinSuccess) {
            threadJoinSuccess.inc();
        }
    }

    @Override
    public void rtQuerySide(long startTime) {
        synchronized (threadRtQuery) {
            long duration = System.nanoTime() - startTime;
            threadRtQuery.update(duration);
        }
    }

    @Override
    public void rpcTimeout() {
        synchronized (threadRpcTimeout) {
            threadRpcTimeout.inc();
        }
    }

    @Override
    public void rpcRetryTimes() {
        synchronized (threadRetryTimes) {
            threadRetryTimes.inc();
        }
    }
}
