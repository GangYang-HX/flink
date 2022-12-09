package org.apache.flink.table.runtime.operators.join.latency.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;
import org.apache.flink.table.runtime.operators.join.latency.util.UnitConstant;
import org.apache.flink.table.runtime.operators.join.latency.compress.Compress;
import org.apache.flink.table.runtime.operators.join.latency.serialize.SerializeService;


import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:10:47 AM
 */
public class HashRedisCache<T> extends JoinCache<T> {

    private long extraExpireMs = 0;
    private JedisCluster jedisCluster;
    private Compress compress;

    public HashRedisCache(String redisAddress, SerializeService<T> serializer, Compress compress, long extraExpireMs,
						  MetricGroup metrics) {
        super(serializer, metrics);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(30);
        poolConfig.setMaxTotal(70);
		poolConfig.setJmxEnabled(false);
        Set<HostAndPort> nodes = new HashSet<>();
        for (String hp : redisAddress.split(SymbolsConstant.COMMA)) {
            String[] split = hp.split(SymbolsConstant.COLON);
            if (split.length != 2) {
                continue;
            }
            nodes.add(new HostAndPort(split[0], Integer.valueOf(split[1])));
        }
        if (CollectionUtils.isEmpty(nodes)) {
            throw new RuntimeException("redis store config is illegal:" + redisAddress);
        }
        this.jedisCluster = new JedisCluster(nodes, 1000, 70, poolConfig);
        this.compress = compress;
        if (extraExpireMs > 0) {
            this.extraExpireMs = extraExpireMs;
        }
    }

    @Override
    protected T doGet(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doPut(String key, T row, long expireMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doDelete(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean doExist(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T doGet(String key, String field) throws IOException {
        long start = System.nanoTime();
        byte[] keyBody = jedisCluster.hget(key.getBytes(), field.getBytes());
        if (metric != null) {
            metric.get.inc();
            metric.getRT.update(System.nanoTime() - start);
        }
        if (keyBody == null) {
            return null;
        }
        byte[] bytes = compress.uncompress(keyBody);
        return serializer.deserialize(bytes);
    }

    @Override
    public void doPut(String key, String field, T row, long expireMs) throws IOException {
        byte[] body = compress.compress(serializer.serialize(row));
        long start = System.nanoTime();
        jedisCluster.hset(key.getBytes(), field.getBytes(), body);
        jedisCluster.expire(key.getBytes(), (int) ((expireMs + extraExpireMs) / UnitConstant.SECONDS2MILLS));
        if (metric != null) {
            metric.put.inc();
            metric.putRT.update(System.nanoTime() - start);
        }
    }

    @Override
    public void doDelete(String key, String field) {
        long start = System.nanoTime();
        jedisCluster.hdel(key.getBytes(), field.getBytes());
        if (metric != null) {
            metric.del.inc();
            metric.delRT.update(System.nanoTime() - start);
        }
    }

    @Override
    public Boolean doExist(String key, String field) {
        long start = System.nanoTime();
        byte[] rs = jedisCluster.hget(key.getBytes(), field.getBytes());
        if (metric != null) {
            metric.exist.inc();
            metric.existRT.update(System.nanoTime() - start);
        }
        return rs != null;
    }

    @Override
    public ScanResult<T> doScan(String key, String scanCursor, Integer count) throws IOException {
        long start = System.nanoTime();
        ScanParams scanParams = new ScanParams();
        scanParams.count(count);
        ScanResult<Map.Entry<byte[], byte[]>> scanResult = jedisCluster.hscan(key.getBytes(), scanCursor.getBytes(), scanParams);
        List<Map.Entry<byte[], byte[]>> result = scanResult.getResult();
        if (CollectionUtils.isEmpty(result)) {
            return new ScanResult(scanResult.getCursor(), Collections.emptyList());
        }

        List<T> data = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> entry: result){
			byte[] bytes = compress.uncompress(entry.getValue());
			data.add(serializer.deserialize(bytes)) ;
		}
        if (metric != null) {
            metric.scan.inc(data.size());
            metric.scanRT.update(System.nanoTime() - start);
        }
        return new ScanResult(scanResult.getCursor(), data);
    }

    @Override
    public void close() throws IOException {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }
}
