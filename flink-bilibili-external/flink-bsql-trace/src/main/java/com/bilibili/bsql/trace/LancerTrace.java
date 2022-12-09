package com.bilibili.bsql.trace;

import com.bilibili.lancer.trace.client.TraceClient;

/** LancerTrace. */
public class LancerTrace implements Trace {
    private static TraceClient traceClient;

    static {
        String traceClientName = "lancer2-collector";
        traceClient =
                new TraceClient(traceClientName, "http://lancer-router.bilibili.co/lancer/trace2");
        traceClient.start();
    }

    public LancerTrace() {}

    @Override
    public void traceEvent(
            String traceId,
            long time,
            long value,
            String key,
            String url,
            int delay,
            String sinkType,
            String pipeline,
            String color) {
        traceClient.traceEventWithDest(
                time, value, key, traceId, url, delay, sinkType, pipeline, color);
    }

    @Override
    public void traceError(
            String traceId,
            long time,
            long value,
            String key,
            String url,
            String error,
            String sinkType,
            String pipeline,
            String color) {
        traceClient.traceErrorWithDest(
                time, value, key, traceId, url, error, sinkType, pipeline, color);
    }
}
