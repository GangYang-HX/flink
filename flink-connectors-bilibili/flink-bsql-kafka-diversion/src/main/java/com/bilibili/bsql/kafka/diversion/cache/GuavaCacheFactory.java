package com.bilibili.bsql.kafka.diversion.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * GuavaCacheFactory.
 *
 * @param <K>
 * @param <V>
 */
public class GuavaCacheFactory<K, V> {

    public Cache<K, V> getCache(long ttl) {
        return CacheBuilder.newBuilder()
                .initialCapacity(10)
                .maximumSize(100)
                .expireAfterWrite(ttl, TimeUnit.MINUTES)
                .concurrencyLevel(1)
                .build();
    }
}
