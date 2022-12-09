package org.apache.flink.table.runtime.operators.join.latency.cache;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.serialize.SerializeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import redis.clients.jedis.ScanResult;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:11:23 AM
 */
public abstract class JoinCache<T> implements Closeable, Serializable {

    private final static Logger         LOG = LoggerFactory.getLogger(JoinCache.class);

    protected final SerializeService<T> serializer;
    protected Metric                    metric;

    public JoinCache(SerializeService<T> serializer, MetricGroup metricGroup) {
        this.serializer = serializer;
        if (metricGroup != null) {
            metric = Metric.getInstance(metricGroup.addGroup("joinCache"));
        }
    }

    public T get(String key) throws IOException {
        long s = System.nanoTime();
        T v = doGet(key);

        if (metric != null) {
            metric.get.inc();
            metric.getRT.update((System.nanoTime() - s) / 1000);
        }
        return v;
    }

    /**
     * doGet
     * 
     * @param key
     * @return
     */
    protected abstract T doGet(String key) throws IOException;

    public void put(String key, T row, long expireMs) throws IOException {
        long s = System.nanoTime();
		doPut(key, row, expireMs);

        if (metric != null) {
            metric.put.inc();
            metric.putRT.update((System.nanoTime() - s) / 1000);
        }
    }

    /**
     * doPut
     * 
     * @param key
     * @param row
     * @param expireMs
     */
    protected abstract void doPut(String key, T row, long expireMs) throws IOException;

    public void delete(String key) {
        long s = System.nanoTime();
		doDelete(key);

        if (metric != null) {
//            metric.del.inc();
            metric.delRT.update((System.nanoTime() - s) / 1000);
        }
    }

    /**
     * doDelete
     * 
     * @param key
     */
    public abstract void doDelete(String key);

    public Boolean exist(String key) {
        long s = System.nanoTime();
        boolean v = doExist(key);
        if (metric != null) {
            metric.exist.inc();
            metric.existRT.update((System.nanoTime() - s) / 1000);
        }
        return v;
    }

    /**
     * doExist
     * 
     * @param key
     * @return
     */
    public abstract Boolean doExist(String key);

    protected abstract T doGet(String key, String field) throws IOException;

    public T get(String key, String field) throws IOException {
        long s = System.nanoTime();
        T v = doGet(key, field);

        if (metric != null) {
            metric.get.inc();
            metric.getRT.update((System.nanoTime() - s) / 1000);
        }
        return v;
    }

    protected abstract void doPut(String key, String field, T row, long expireMs) throws IOException;

    public void put(String key, String field, T row, long expireMs) throws IOException {
        long s = System.nanoTime();
		doPut(key, field, row, expireMs);

        if (metric != null) {
            metric.put.inc();
            metric.putRT.update((System.nanoTime() - s) / 1000);
        }
    }

    protected abstract void doDelete(String key, String field);

    public void delete(String key, String field) {
        long s = System.nanoTime();
		doDelete(key, field);

        if (metric != null) {
//            metric.del.inc();
            metric.delRT.update((System.nanoTime() - s) / 1000);
        }
    }

    public Boolean exist(String key, String field) {
        long s = System.nanoTime();
        boolean v = doExist(key, field);
        if (metric != null) {
            metric.exist.inc();
            metric.existRT.update((System.nanoTime() - s) / 1000);
        }
        return v;
    }

    protected abstract ScanResult<T> doScan(String key, String scanCursor, Integer count) throws IOException;

    public ScanResult<T> scan(String key, String scanCursor, Integer count) throws IOException {
        long s = System.nanoTime();
        ScanResult<T> result = doScan(key, scanCursor, count);

        if (metric != null) {
            metric.scan.inc();
            metric.scanRT.update((System.nanoTime() - s) / 1000);
        }
        return result;
    }

    /**
     * doExist
     *
     * @param key
     * @return
     */
    protected abstract Boolean doExist(String key, String field);


}
