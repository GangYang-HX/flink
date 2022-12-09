package com.bilibili.bsql.common.cache.cacheobj;

import org.apache.flink.table.data.RowData;

import java.util.List;

/** CacheObj. */
public abstract class CacheObj<T> {

    public T content;

    public CacheObj(T content) {
        this.content = content;
    }

    public static CacheObj buildCacheObj(CacheContentType type, Object content) {
        if (type == CacheContentType.MissVal) {
            return CacheMissVal.getMissKeyObj();
        } else if (type == CacheContentType.SingleLine) {
            return new SingleCacheObj((RowData) content);
        } else if (type == CacheContentType.MultiLine) {
            return new MultipleCacheObj((List<RowData>) content);
        } else {
            throw new UnsupportedOperationException("only three content type exist");
        }
    }

    public abstract CacheContentType getType();

    public abstract T getContent();

    public abstract void setContent(T content);
}
