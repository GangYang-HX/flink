package com.bilibili.bsql.trace;

/** DefaultTrace. */
public class DefaultTrace implements Trace {

    public DefaultTrace() {}

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
            String color) {}

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
            String color) {}
}
