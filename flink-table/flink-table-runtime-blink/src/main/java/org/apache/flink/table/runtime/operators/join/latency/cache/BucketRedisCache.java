package org.apache.flink.table.runtime.operators.join.latency.cache;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.util.MurmurHashUtils;
import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;
import org.apache.flink.table.runtime.operators.join.latency.compress.Compress;
import org.apache.flink.table.runtime.operators.join.latency.serialize.SerializeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;

/**
 * <pre>
 *     将所有的kv按照md5(k)分散到多个bucket里面,每个bucket里面持有部分kv,bucket在小于一定的容量时候,
 *     底层会使用zipList结构,压缩比比较高,达到节省内存的目的,不过过期机制就时效了,hset接口不支持过期,
 *     未开启从checkpoint进行recovery的重启,可能带来巨大的风险。
 * </pre>
 *
 * @author zhangyang
 * @Date:2019/11/6
 * @Time:10:24 AM
 */
public class BucketRedisCache<T> extends JoinCache<T> {

    private final static Logger LOG = LoggerFactory.getLogger(BucketRedisCache.class);
    private final Compress compress;
    private long                bucketNum;
    private JedisCluster        jedisCluster;
    private long                extraExpireMs;

    public BucketRedisCache(String redisAddress, SerializeService<T> serializer, Compress compress, long extraExpireMs,
							long bucketNum, MetricGroup metrics) {
        super(serializer, metrics);
		long start = Clock.systemUTC().millis();
        this.compress = compress;
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
            nodes.add(new HostAndPort(split[0], Integer.parseInt(split[1])));
        }
        if (CollectionUtils.isEmpty(nodes)) {
            throw new RuntimeException("redis store config is illegal:" + redisAddress);
        }
        this.jedisCluster = new JedisCluster(nodes, 1000, 70, poolConfig);
        assert extraExpireMs > 0;
        assert bucketNum > 0;
        this.extraExpireMs = extraExpireMs;
        this.bucketNum = bucketNum;
		long startCost = Clock.systemUTC().millis() - start;
        LOG.info("redisAddress:{} , bucketNum:{}. cost: {} ", redisAddress, bucketNum, startCost);
    }

    @Override
    public T doGet(String key) throws IOException {
        byte[] bucket = getBucketId(key);
        long start = System.nanoTime();
        byte[] body = jedisCluster.hget(bucket, key.getBytes());
        if (metric != null) {
            metric.getRT.update(System.nanoTime() - start);
            metric.get.inc();
        }
        if (body == null) {
            return null;
        }
        byte[] bytes = compress.uncompress(body);
        return serializer.deserialize(bytes);
    }

    @Override
    public void doPut(String key, T row, long expireMs) throws IOException {
        byte[] serBytes = serializer.serialize(row);
        byte[] body = compress.compress(serBytes);
        byte[] bucket = getBucketId(key);
        long start = System.nanoTime();
        jedisCluster.hset(bucket, key.getBytes(), body);
        if (metric != null) {
            metric.putRT.update(System.nanoTime() - start);
            metric.put.inc();
        }
    }

    @Override
    public void doDelete(String key) {
        byte[] bucket = getBucketId(key);
        long start = System.nanoTime();
        long affectedKey = jedisCluster.hdel(bucket, key.getBytes());
        if (metric != null) {
            metric.delRT.update(System.nanoTime() - start);
            if (affectedKey >0) {
            	metric.del.inc();
			}
        }
    }

    @Override
    public Boolean doExist(String key) {
        byte[] bucket = getBucketId(key);
        long start = System.nanoTime();
        byte[] bytes = jedisCluster.hget(bucket, key.getBytes());
        if (metric != null) {
            metric.existRT.update(System.nanoTime() - start);
            metric.exist.inc();
        }
        return bytes != null;
    }

    @Override
    protected T doGet(String key, String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doPut(String key, String field, T row, long expireMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doDelete(String key, String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected ScanResult<T> doScan(String key, String scanCursor, Integer count) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Boolean doExist(String key, String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }

    private byte[] getBucketId(String key) {
        long start = System.nanoTime();
        long hashKey = Math.abs(MurmurHashUtils.hash64(key));
        byte[] bucket = String.valueOf(hashKey % bucketNum).getBytes();
        if (metric != null) {
            metric.bucketRT.update(System.nanoTime() - start);
            metric.bucket.inc();
        }
        return bucket;
    }
}
