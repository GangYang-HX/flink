/** Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved. */
package com.bilibili.bsql.redis.source.keyhandler;

import io.lettuce.core.RedisFuture;

/** @version $Id: KeyGenerator.java */
public interface KeyGenerator<K, V> {
    String createCacheKey(Object... inputs);

    K createKeyFromInput(Object... inputs);

    boolean shouldReturnDirectly(Object... inputs);

    RedisFuture<V> queryRedis(K key);
}
