package org.apache.flink.table.runtime.operators.join.latency.serialize;

import java.io.IOException;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.runtime.operators.join.latency.DefaultMetricsConfig;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:11:05 AM
 */
public abstract class SerializeService<T> {

    private Metric metric;

    public SerializeService(MetricGroup metricGroup) {
        if (metricGroup != null) {
            metric = new Metric(metricGroup.addGroup("serialize"));
        }
    }

    /**
     * serialize
     * 
     * @param t
     * @return
     */
    public byte[] serialize(T t) throws IOException{
        long start = System.nanoTime();
        byte[] serializeBytes = doSerialize(t);
        if (metric != null) {
            metric.serializeRT.update(System.nanoTime() - start);
            metric.serialize.inc();
        }
        return serializeBytes;
    }

    /**
     * doSerialize
     * 
     * @param t
     * @return
     */
    protected abstract byte[] doSerialize(T t) throws IOException;

    /**
     * deserialize
     * 
     * @param bytes
     * @return
     */
    public T deserialize(byte[] bytes) throws IOException{
        long start = System.nanoTime();
        T t = doDeserialize(bytes);
        if (metric != null) {
            metric.deSerializeRT.update(System.nanoTime() - start);
            metric.deSerialize.inc();
        }
        return t;
    }

    /**
     * doDeserialize
     * 
     * @param bytes
     * @return
     */
    protected abstract T doDeserialize(byte[] bytes) throws IOException;

    private static class Metric {

        private Counter serialize;
        private Histogram serializeRT;
        private Counter deSerialize;
        private Histogram deSerializeRT;

        private Metric(MetricGroup metricGroup) {
            serialize = metricGroup.counter("serialize");
            serializeRT = metricGroup.histogram("serializeRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
            deSerialize = metricGroup.counter("deSerialize");
            deSerializeRT = metricGroup.histogram("deSerializeRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
        }
    }
}
