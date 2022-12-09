package org.apache.flink.table.runtime.operators.join.latency.compress.fastgzip;

/**
 * @author zhangyang
 * @Date:2019/12/12
 * @Time:2:04 PM
 */
public interface ReusableStream {

    /**
     * reset
     */
    void reset();
}
