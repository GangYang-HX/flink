package com.bilibili.bsql.mysql.table;

import com.bilibili.bsql.common.table.RdbLookupOptionsEntity;

/** BsqlMysqlLookupOptionsEntity. */
public class BsqlMysqlLookupOptionsEntity extends RdbLookupOptionsEntity {

    private int idleConnectionTestPeriodType;
    private int maxIdleTime;
    private int tmConnectionPoolSize;

    public BsqlMysqlLookupOptionsEntity(
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
            String diverName,
            int idleConnectionTestPeriodType,
            int maxIdleTime,
            int tmConnectionPoolSize) {
        super(
                tableName,
                tableType,
                cacheSize,
                cacheTimeout,
                cacheType,
                multikeyDelimiter,
                url,
                password,
                username,
                connectionPoolSize,
                diverName);
        this.idleConnectionTestPeriodType = idleConnectionTestPeriodType;
        this.maxIdleTime = maxIdleTime;
        this.tmConnectionPoolSize = tmConnectionPoolSize;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public int idleConnectionTestPeriodType() {
        return idleConnectionTestPeriodType;
    }

    public int maxIdleTime() {
        return maxIdleTime;
    }

    public int tmConnectionPoolSize() {
        return tmConnectionPoolSize;
    }

    /** BsqlMysqlLookupOptionsEntity Builder. */
    public static final class Builder {
        private int cacheSize;
        private long cacheTimeout;
        private String cacheType;
        private String multikeyDelimiter;
        private String tableName;
        private String tableType;
        private String url;
        private String password;
        private String username;
        private int connectionPoolSize;
        private String diverName;
        private int idleConnectionTestPeriodType;
        private int maxIdleTime;
        private int tmConnectionPoolSize;

        private Builder() {}

        public Builder cacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder cacheTimeout(long cacheTimeout) {
            this.cacheTimeout = cacheTimeout;
            return this;
        }

        public Builder cacheType(String cachType) {
            this.cacheType = cachType;
            return this;
        }

        public Builder multikeyDelimiter(String multikeyDelimiter) {
            this.multikeyDelimiter = multikeyDelimiter;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder tableType(String tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder connectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public Builder diverName(String diverName) {
            this.diverName = diverName;
            return this;
        }

        public Builder idleConnectionTestPeriodType(int idleConnectionTestPeriodType) {
            this.idleConnectionTestPeriodType = idleConnectionTestPeriodType;
            return this;
        }

        public Builder maxIdleTime(int maxIdleTime) {
            this.maxIdleTime = maxIdleTime;
            return this;
        }

        public Builder tmConnectionPoolSize(int tmConnectionPoolSize) {
            this.tmConnectionPoolSize = tmConnectionPoolSize;
            return this;
        }

        public BsqlMysqlLookupOptionsEntity build() {
            return new BsqlMysqlLookupOptionsEntity(
                    tableName,
                    tableType,
                    cacheSize,
                    cacheTimeout,
                    cacheType,
                    multikeyDelimiter,
                    url,
                    password,
                    username,
                    connectionPoolSize,
                    diverName,
                    idleConnectionTestPeriodType,
                    maxIdleTime,
                    tmConnectionPoolSize);
        }
    }
}
