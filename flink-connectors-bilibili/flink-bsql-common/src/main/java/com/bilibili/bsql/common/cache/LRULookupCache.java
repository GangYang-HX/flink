package com.bilibili.bsql.common.cache;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import com.bilibili.bsql.common.cache.cacheobj.CacheObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/** LRUSideCache. */
public class LRULookupCache extends LookupCacheBase {

    private static final Logger LOG = LoggerFactory.getLogger(LRULookupCache.class);

    protected transient Cache<Object, CacheObj> cache;

    public LRULookupCache(int cacheSize, long cacheTimeout, String cacheType) {
        super(cacheSize, cacheTimeout, cacheType);
        initCache();
    }

    @Override
    public void initCache() {
        // 当前只有LRU
        cache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfterWrite(cacheTimeout, TimeUnit.MILLISECONDS)
                        .build();
        LOG.info("init LRULookupCache success");
    }

    @Override
    public CacheObj getFromCache(Object key) {
        if (cache == null) {
            return null;
        }
        return cache.getIfPresent(key);
    }

    @Override
    public void putCache(Object key, CacheObj value) {
        if (cache == null) {
            return;
        }
        cache.put(key, value);
    }

    @Override
    public String toString() {
        return "LRULookupCache{" + "cache=" + cache.asMap() + '}';
    }

    public Map<Object, CacheObj> getFullCache() {
        return cache.asMap();
    }
}
