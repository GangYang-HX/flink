package com.bilibili.bsql.common.table;

import java.io.Serializable;

/** RdbLookupOptionsEntity. */
public class RdbLookupOptionsEntity extends LookupOptionsEntityBase implements Serializable {

    private static final long serialVersionUID = 1L;
    protected String url;
    protected String password;
    protected String username;
    protected int connectionPoolSize;
    protected String diverName;

    public RdbLookupOptionsEntity(
            String tableName,
            String tableType,
            int cacheSize,
            long cacheTimeout,
            String cacheType,
            String multikeyDelimiter,
            String url,
            String password,
            String username,
            int connectionPoolSize,
            String diverName) {
        super(tableName, tableType, cacheSize, cacheTimeout, cacheType, multikeyDelimiter);
        this.url = url;
        this.password = password;
        this.username = username;
        this.connectionPoolSize = connectionPoolSize;
        this.diverName = diverName;
    }

    public String url() {
        return url;
    }

    public String password() {
        return password;
    }

    public String username() {
        return username;
    }

    public int connectionPoolSize() {
        return connectionPoolSize;
    }

    public String diverName() {
        return diverName;
    }
}
