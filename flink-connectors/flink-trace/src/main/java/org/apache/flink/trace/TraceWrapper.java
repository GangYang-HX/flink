package org.apache.flink.trace;

/**
 * @author FredGao
 * @version 1.0.0
 * @ClassName TraceWrapper.java
 * @Description
 * @createTime 2022-01-28 16:15:00
 */
public interface TraceWrapper {

    void setTrace(Trace trace);
}
