package com.bilibili.bsql.redis.sink.jedisbin;



import com.bilibili.bsql.redis.sink.jedisbin.clusterpipeline.ClusterPipeline;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;

/**
 * cluster的pipelined模式,不支持集群热更新
 * 
 * @author zhangyang
 * @Date:2020/3/16
 * @Time:12:11 PM
 */
public class JedisClusterPipeline extends JedisBinary {

    private ClusterPipeline clusterPipeline;
    private final static String OK = "ok";

    public JedisClusterPipeline(JedisCluster jedisCluster) {
        this.clusterPipeline = new ClusterPipeline(jedisCluster);
    }

    @Override
    public String set(byte[] key, byte[] value) {
        clusterPipeline.set(key, value);
        return OK;
    }

    @Override
    public String set(String key, String value) {
        clusterPipeline.set(key, value);
        return OK;
    }

    @Override
    public String set(byte[] key, String value) {
        clusterPipeline.set(key, value.getBytes());
        return OK;
    }

    @Override
    public String set(String key, byte[] value) {
        clusterPipeline.set(key.getBytes(), value);
        return OK;
    }

    @Override
    public String setex(byte[] key, int expire, byte[] value) {
        clusterPipeline.setex(key, expire, value);
        return OK;
    }

    @Override
    public String setex(String key, int expire, String value) {
        clusterPipeline.setex(key, expire, value);
        return OK;
    }

    @Override
    public String setex(byte[] key, int expire, String value) {
        clusterPipeline.setex(key, expire, value.getBytes());
        return OK;
    }

    @Override
    public String setex(String key, int expire, byte[] value) {
        clusterPipeline.setex(key.getBytes(), expire, value);
        return OK;
    }

    @Override
    public Long hsetex(byte[] key, int expire, byte[] field, byte[] value) {
        Response<Long> result = clusterPipeline.hset(key, field, value);
        clusterPipeline.expire(key,expire);
        return 0L;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return clusterPipeline.hset(key, field, value).get();
    }

    @Override
    public void close() {
        clusterPipeline.close();
    }

    @Override
    public void sync() {
        clusterPipeline.sync();
    }
}
