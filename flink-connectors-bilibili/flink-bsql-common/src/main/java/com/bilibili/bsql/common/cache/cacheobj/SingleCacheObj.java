package com.bilibili.bsql.common.cache.cacheobj;

import org.apache.flink.table.data.RowData;

/** SingleCacheObj. */
public class SingleCacheObj extends CacheObj<RowData> {

    public SingleCacheObj(RowData content) {
        super(content);
    }

    public CacheContentType getType() {
        return CacheContentType.SingleLine;
    }

    public RowData getContent() {
        return content;
    }

    public void setContent(RowData content) {
        this.content = content;
    }
}
