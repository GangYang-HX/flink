package com.bilibili.bsql.trace;

import java.io.Serializable;

/** LogTracer. */
public interface LogTracer extends Serializable {

    default void logTraceEvent(
            Trace trace,
            String traceId,
            long time,
            long value,
            String key,
            String url,
            int delay,
            String sinkDest,
            String pipeline,
            String color) {
        trace.traceEvent(traceId, time, value, key, url, delay, sinkDest, pipeline, color);
    }

    default void logTraceError(
            Trace trace,
            String traceId,
            long time,
            long value,
            String key,
            String url,
            String errorInfo,
            String sinkDest,
            String pipeline,
            String color) {
        trace.traceError(traceId, time, value, key, url, errorInfo, sinkDest, pipeline, color);
    }
}
