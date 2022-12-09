package com.bilibili.bsql.common.cache;

import com.bilibili.bsql.common.cache.cacheobj.CacheObj;

/** SideCacheBase. */
public abstract class LookupCacheBase {

    protected int cacheSize;
    protected long cacheTimeout;
    protected String cacheType;

    public LookupCacheBase(int cacheSize, long cacheTimeout, String cacheType) {
        this.cacheSize = cacheSize;
        this.cacheTimeout = cacheTimeout;
        this.cacheType = cacheType;
    }

    public abstract void initCache();

    public abstract CacheObj getFromCache(Object key);

    public abstract void putCache(Object key, CacheObj value);

    @Override
    public String toString() {
        return "LookupCacheBase{"
                + "cacheSize="
                + cacheSize
                + ", cacheTimeout="
                + cacheTimeout
                + ", cacheType='"
                + cacheType
                + '\''
                + '}';
    }
}
