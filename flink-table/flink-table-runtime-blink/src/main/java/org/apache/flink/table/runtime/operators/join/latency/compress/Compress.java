package org.apache.flink.table.runtime.operators.join.latency.compress;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.runtime.operators.join.latency.DefaultMetricsConfig;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:11:12 AM
 */
public abstract class Compress {

    private Metric metric;

    public Compress(MetricGroup metricGroup) {
        if (metricGroup != null) {
            metric = new Metric(metricGroup.addGroup("compress"));
        }
    }

    /**
     * compress
     * 
     * @param bytes
     * @return
     */
    public byte[] compress(byte[] bytes) {
        long start = System.nanoTime();
        byte[] rs = doCompress(bytes);
        if (metric != null) {
            metric.compressRT.update(System.nanoTime() - start);
            metric.compress.inc();
        }
        return rs;
    }

    /**
     * doCompress
     * 
     * @param bytes
     * @return
     */
    protected abstract byte[] doCompress(byte[] bytes);

    /**
     * uncompress
     *
     * @param bytes
     * @return
     */
    public byte[] uncompress(byte[] bytes) {
        long start = System.nanoTime();
        byte[] rs = doUncompress(bytes);
        if (metric != null) {
            metric.uncompressRT.update(System.nanoTime() - start);
            metric.uncompress.inc();
        }
        return rs;
    }

    /**
     * doUncompress
     * 
     * @param bytes
     * @return
     */
    protected abstract byte[] doUncompress(byte[] bytes);

    private static class Metric {

        private Counter compress;
        private Histogram compressRT;
        private Counter uncompress;
        private Histogram uncompressRT;

        private Metric(MetricGroup metricGroup) {
            compress = metricGroup.counter("compress");
            compressRT = metricGroup.histogram("compressRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
            uncompress = metricGroup.counter("uncompress");
            uncompressRT = metricGroup.histogram("uncompressRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
        }
    }
}
