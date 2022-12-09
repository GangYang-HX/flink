package com.bilibili.bsql.common.table;

import java.io.Serializable;

/** LookupOptionsEntityBase. */
public class LookupOptionsEntityBase implements Serializable {
    protected int cacheSize;
    protected long cacheTimeout;
    protected String cacheType = "none";

    protected String multikeyDelimiter;
    protected String tableName;
    protected String tableType;

    public LookupOptionsEntityBase(
            String tableName,
            String tableType,
            int cacheSize,
            long cacheTimeout,
            String cacheType,
            String multikeyDelimiter) {
        this.tableName = tableName;
        this.tableType = tableType;
        this.cacheSize = cacheSize;
        this.cacheTimeout = cacheTimeout;
        this.cacheType = cacheType;
        this.multikeyDelimiter = multikeyDelimiter;
    }

    public int cacheSize() {
        return cacheSize;
    }

    public long cacheTimeout() {
        return cacheTimeout;
    }

    public String cacheType() {
        return cacheType;
    }

    public String multikeyDelimiter() {
        return multikeyDelimiter;
    }

    public String tableName() {
        return tableName;
    }

    public String tableType() {
        return tableType;
    }
}
