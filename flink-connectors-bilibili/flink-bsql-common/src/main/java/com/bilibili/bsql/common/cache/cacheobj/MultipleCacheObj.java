package com.bilibili.bsql.common.cache.cacheobj;

import org.apache.flink.table.data.RowData;

import java.util.List;

/** MultipleCacheObj. */
public class MultipleCacheObj extends CacheObj<List<RowData>> {

    public MultipleCacheObj(List<RowData> content) {
        super(content);
    }

    public CacheContentType getType() {
        return CacheContentType.MultiLine;
    }

    public List<RowData> getContent() {
        return content;
    }

    public void setContent(List<RowData> content) {
        this.content = content;
    }
}
