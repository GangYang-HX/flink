package org.apache.flink.table.runtime.operators.join.latency.cache;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.util.MurmurHashUtils;
import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;
import org.apache.flink.table.runtime.operators.join.latency.compress.Compress;
import org.apache.flink.table.runtime.operators.join.latency.serialize.SerializeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
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
public class LettuceRedisCache<T> extends JoinCache<T> {

    private final static Logger                          LOG = LoggerFactory.getLogger(LettuceRedisCache.class);
    private final Compress compress;
    private long                                         bucketNum;
    StatefulRedisClusterConnection<byte[], byte[]>       connect;
    private RedisAdvancedClusterCommands<byte[], byte[]> command;
    private long                                         extraExpireMs;

    public LettuceRedisCache(String redisAddress, SerializeService<T> serializer, Compress compress,
							 long extraExpireMs, long bucketNum, MetricGroup metrics) {
        super(serializer, metrics);

        this.compress = compress;

        ArrayList<RedisURI> list = new ArrayList<>();
        for (String address : redisAddress.split(SymbolsConstant.COMMA)) {
            list.add(RedisURI.create("redis://" + address));
        }
        RedisClusterClient client = RedisClusterClient.create(list);
        connect = client.connect(ByteArrayCodec.INSTANCE);
        command = connect.sync();
        assert extraExpireMs > 0;
        assert bucketNum > 0;
        this.extraExpireMs = extraExpireMs;
        this.bucketNum = bucketNum;
        LOG.info("bucketNum:{}", bucketNum);
    }

    @Override
    public T doGet(String key) throws IOException {
        long start = System.nanoTime();
        byte[] body = command.hget(getBucketId(key), key.getBytes());
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
        command.hset(bucket, key.getBytes(), body);
        if (metric != null) {
            metric.putRT.update(System.nanoTime() - start);
            metric.put.inc();
        }
    }

    @Override
    public void doDelete(String key) {
        byte[] bucket = getBucketId(key);
        long start = System.nanoTime();
        command.hdel(bucket, key.getBytes());
        if (metric != null) {
            metric.delRT.update(System.nanoTime() - start);
            metric.del.inc();
        }
    }

    @Override
    public Boolean doExist(String key) {
        byte[] bucket = getBucketId(key);
        long start = System.nanoTime();
        byte[] rs = command.hget(bucket, key.getBytes());
        if (metric != null) {
            metric.existRT.update(System.nanoTime() - start);
            metric.exist.inc();
        }
        return rs != null;
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
        if (connect != null) {
            connect.close();
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
