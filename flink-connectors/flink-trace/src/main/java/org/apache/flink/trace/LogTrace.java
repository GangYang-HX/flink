package org.apache.flink.trace;

import org.apache.flink.trace.Trace;

import java.io.Serializable;
import java.util.Map;

/**
 * @author FredGao
 * @createTime 2022-05-17 17:08:00
 */
public interface LogTrace extends Serializable {

    void setHeaders(Map<String, String> headers);


    default void logTraceError(Trace trace, Map<String, String> headers, String error, String sinkDest){
        long currentTime = System.currentTimeMillis();
        trace.traceError(
                Long.parseLong(headers.getOrDefault( "ctime", String.valueOf(currentTime))),
                1,
                "internal.error",
                "",
                error,
                sinkDest
        );
    }

    default void logTraceEvent(Trace trace, Map<String, String> headers, String sinkDest){
        long currentTime = System.currentTimeMillis();
        trace.traceEvent(
                Long.parseLong(headers.getOrDefault( "ctime", String.valueOf(currentTime))),
                1,
                "batch.input.receive",
                "",
                (int) (currentTime - Long.parseLong(headers.getOrDefault( "utime", String.valueOf(currentTime)))),
                sinkDest
        );
    }
}
