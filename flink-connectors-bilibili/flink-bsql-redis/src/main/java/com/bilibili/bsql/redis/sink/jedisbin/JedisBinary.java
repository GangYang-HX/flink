/**
 * Bilibili.com Inc. Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.redis.sink.jedisbin;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;

import redis.clients.jedis.util.SafeEncoder;

/**
 * @author zhouxiaogang
 * @version $Id: JedisBinary.java, v 0.1 2019-11-20 11:23 zhouxiaogang Exp $$
 */
public abstract class JedisBinary {

    public abstract String set(byte[] key, byte[] value);

    public abstract String set(String key, String value);

    public abstract String set(byte[] key, String value);

    public abstract String set(String key, byte[] value);

    public String set(Object key, Object value) {
        if (key instanceof Byte[]) {
            if (value instanceof Byte[]) {
                return this.set(ArrayUtils.toPrimitive((Byte[]) key), ArrayUtils.toPrimitive((Byte[]) value));
            } else if (value instanceof byte[]) {
                return this.set(ArrayUtils.toPrimitive((Byte[]) key), ArrayUtils.toObject((byte[]) value));
            } else {
                return this.set(ArrayUtils.toPrimitive((Byte[]) key), value.toString());
            }
        } else {
            if (value instanceof Byte[]) {
                return this.set(key.toString(), ArrayUtils.toPrimitive((Byte[]) value));
            } else if (value instanceof byte[]) {
                return this.set(key.toString(), ArrayUtils.toObject((byte[]) value));
            } else {
                return this.set(key.toString(), value.toString());
            }
        }
    }

    public abstract String setex(byte[] key, int expire, byte[] value);

    public abstract String setex(String key, int expire, String value);

    public abstract String setex(byte[] key, int expire, String value);

    public abstract String setex(String key, int expire, byte[] value);

    public abstract Long hsetex(byte[] key, int expire, byte[] field, byte[] value);

    public abstract Long hset(byte[] key, byte[] field, byte[] value);

    public String setex(Object key, int expire, Object value) {
        if (key instanceof Byte[]) {
            if (value instanceof Byte[]) {
                return this.setex(ArrayUtils.toPrimitive((Byte[]) key), expire, ArrayUtils.toPrimitive((Byte[]) value));
            } else if (value instanceof byte[]) {
                return this.setex(ArrayUtils.toPrimitive((Byte[]) key), expire, ArrayUtils.toObject((byte[]) value));
            } else {
                return this.setex(ArrayUtils.toPrimitive((Byte[]) key), expire, value.toString());
            }
        } else {
            if (value instanceof Byte[]) {
                return this.setex(key.toString(), expire, ArrayUtils.toPrimitive((Byte[]) value));
            } else if (value instanceof byte[]) {
                return this.setex(key.toString(), expire, ArrayUtils.toObject((byte[]) value));
            } else {
                return this.setex(key.toString(), expire, value.toString());
            }
        }
    }

    public void sync() {
        // nothing to do expect pipeline jedis cluster
    }

    public abstract void close() throws IOException;

    public Long hsetex(Object key, int expire, Object field, Object value) {
        return this.hsetex(byteConversion(key), expire, byteConversion(field), byteConversion(value));
    }

    public Long hset(Object key, Object field, Object value) {
        return this.hset(byteConversion(key), byteConversion(field), byteConversion(value));
    }

    private byte[] byteConversion(Object object) {
        byte[] result;
        if (object instanceof Byte[]) {
            result = ArrayUtils.toPrimitive((Byte[]) object);
        } else {
            result = SafeEncoder.encode(object.toString());
        }
        return result;
    }
}
