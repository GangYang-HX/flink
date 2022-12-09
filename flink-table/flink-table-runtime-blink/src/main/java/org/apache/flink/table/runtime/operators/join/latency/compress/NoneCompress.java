package org.apache.flink.table.runtime.operators.join.latency.compress;

import org.apache.flink.metrics.MetricGroup;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:11:14 AM
 */
public class NoneCompress extends Compress {

    public NoneCompress(MetricGroup metricGroup) {
        super(metricGroup);
    }

    @Override
    public byte[] doCompress(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] doUncompress(byte[] bytes) {
        return bytes;
    }
}
