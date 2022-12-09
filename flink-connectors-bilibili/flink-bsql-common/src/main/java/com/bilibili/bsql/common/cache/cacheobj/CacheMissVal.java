package com.bilibili.bsql.common.cache.cacheobj;

import org.apache.flink.table.data.RowData;

/** CacheMissVal. */
public class CacheMissVal extends CacheObj<RowData> {

    private static CacheObj missObj = new CacheMissVal(null);

    public static CacheObj getMissKeyObj() {
        return missObj;
    }

    public CacheMissVal(RowData content) {
        super(content);
    }

    public CacheContentType getType() {
        return CacheContentType.MissVal;
    }

    public RowData getContent() {
        return null;
    }

    public void setContent(RowData content) {
        throw new UnsupportedOperationException();
    }
}
