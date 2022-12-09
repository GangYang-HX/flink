/**
 * Bilibili.com Inc. Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.redis.sink.jedisbin;

import redis.clients.jedis.JedisCluster;

/**
 * @author zhouxiaogang
 * @version $Id: ClusterJedis.java, v 0.1 2019-11-20 14:05
 */
public class ClusterJedis extends JedisBinary {

    private JedisCluster jedisBinary;

    public ClusterJedis(JedisCluster jedisBinary) {
        this.jedisBinary = jedisBinary;
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return jedisBinary.set(key, value);
    }

    @Override
    public String set(String key, String value) {
        return jedisBinary.set(key, value);
    }

    @Override
    public String set(byte[] key, String value) {
        return jedisBinary.set(key, value.getBytes());
    }

    @Override
    public String set(String key, byte[] value) {
        return jedisBinary.set(key.getBytes(), value);
    }

    @Override
    public String setex(byte[] key, int expire, byte[] value) {
        return jedisBinary.setex(key, expire, value);
    }

    @Override
    public String setex(String key, int expire, String value) {
        return jedisBinary.setex(key, expire, value);
    }

    @Override
    public String setex(byte[] key, int expire, String value) {
        return jedisBinary.setex(key, expire, value.getBytes());
    }

    @Override
    public String setex(String key, int expire, byte[] value) {
        return jedisBinary.setex(key.getBytes(), expire, value);
    }

    @Override
    public Long hsetex(byte[] key, int expire,byte[] field, byte[] value){
        Long result = jedisBinary.hset(key, field, value);
        jedisBinary.expire(key,expire);
        return result;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return jedisBinary.hset(key, field, value);
    }

    @Override
    public void close() {
        jedisBinary.close();
    }
}
