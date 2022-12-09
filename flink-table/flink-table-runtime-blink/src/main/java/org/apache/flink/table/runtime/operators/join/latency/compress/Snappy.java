package org.apache.flink.table.runtime.operators.join.latency.compress;

import java.io.IOException;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:11:13 AM
 */
public class Snappy extends Compress {

    private static final Logger LOG = LoggerFactory.getLogger(Snappy.class);

    public Snappy(MetricGroup metricGroup) {
        super(metricGroup);
    }

    @Override
    public byte[] doCompress(byte[] bytes) {
        try {
            return org.xerial.snappy.Snappy.compress(bytes);
        } catch (IOException e) {
            LOG.error("compress error:", e);
        }
        return null;
    }

    @Override
    public byte[] doUncompress(byte[] bytes) {
        try {
            return org.xerial.snappy.Snappy.uncompress(bytes);
        } catch (IOException e) {
            LOG.error("uncompress error:", e);
        }
        return null;
    }
}
