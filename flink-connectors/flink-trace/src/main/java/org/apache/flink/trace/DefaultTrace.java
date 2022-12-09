package org.apache.flink.trace;


/**
 * @author FredGao
 * @version 1.0.0
 * @ClassName TraceWrapper.java
 * @Description
 * @createTime 2022-01-27 14:50:00
 */
public class DefaultTrace implements Trace {

    private final String traceId;

    public DefaultTrace(String traceId) {
        this.traceId = traceId;
    }




    @Override
    public void traceEvent(long time, long value, String key, String url, int delay, String sinkType) {
        //do nothing
    }

    @Override
    public void traceError(long time, long value, String key, String url, String error, String sinkType) {
        //do nothing
    }
}
