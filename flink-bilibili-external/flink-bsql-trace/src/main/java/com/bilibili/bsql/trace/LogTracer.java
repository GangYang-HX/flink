package com.bilibili.bsql.trace;

import java.io.Serializable;
import java.util.Map;

/** LogTracer. */
public interface LogTracer extends Serializable {

    default void setHeaders(Map<String, String> headers) {};

    default void setTraceParam(String traceId, String sinkDest) {};

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
