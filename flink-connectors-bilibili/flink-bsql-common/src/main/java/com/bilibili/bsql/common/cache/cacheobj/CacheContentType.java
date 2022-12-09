package com.bilibili.bsql.common.cache.cacheobj;

/** CacheContentType. */
public enum CacheContentType {
    MissVal(0),
    SingleLine(1),
    MultiLine(2);

    int type;

    CacheContentType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }
}
