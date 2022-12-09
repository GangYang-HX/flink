package com.bilibili.bsql.redis;

import com.bilibili.bsql.common.table.LookupOptionsEntityBase;

/**
 * @version BsqlRedisLookupOptionsEntity.java
 * @see LookupOptionsEntityBase
 */
public class BsqlRedisLookupOptionsEntity extends LookupOptionsEntityBase {
    private String url;
    private String password;
    private String username;
    private Integer redisType;

    private boolean multiKey;

    private Integer keyIndex;

    private Integer retryMaxNum;
    private Long queryTimeOut;

    public BsqlRedisLookupOptionsEntity(
            String tableName,
            String tableType,
            int cacheSize,
            long cacheTimeout,
            String cacheType,
            String multikeyDelimiter,
            String url,
            String password,
            String username,
            Integer redisType,
            boolean multiKey,
            Integer keyIndex,
            Integer retryMaxNum,
            Long queryTimeOut) {
        super(tableName, tableType, cacheSize, cacheTimeout, cacheType, multikeyDelimiter);
        this.url = url;
        this.password = password;
        this.username = username;
        this.redisType = redisType;
        this.multiKey = multiKey;
        this.keyIndex = keyIndex;
        this.retryMaxNum = retryMaxNum;
        this.queryTimeOut = queryTimeOut;
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

    public Integer redisType() {
        return redisType;
    }

    public boolean multiKey() {
        return multiKey;
    }

    public Integer keyIndex() {
        return keyIndex;
    }

    public Integer retryMaxNum() {
        return retryMaxNum;
    }

    public Long queryTimeOut() {
        return queryTimeOut;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** @version com.bilibili.bsql.redis.BsqlRedisLookupOptionsEntity.Builder */
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
        private Integer redisType;
        private boolean multiKey;
        private Integer keyIndex;
        private Integer retryMaxNum;
        private Long queryTimeOut;

        public Builder setCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder setCacheTimeout(long cacheTimeout) {
            this.cacheTimeout = cacheTimeout;
            return this;
        }

        public Builder setCacheType(String cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        public Builder setMultikeyDelimiter(String multikeyDelimiter) {
            this.multikeyDelimiter = multikeyDelimiter;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setTableType(String tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setRedisType(Integer redisType) {
            this.redisType = redisType;
            return this;
        }

        public Builder setMultiKey(boolean multiKey) {
            this.multiKey = multiKey;
            return this;
        }

        public Builder setKeyIndex(Integer keyIndex) {
            this.keyIndex = keyIndex;
            return this;
        }

        public Builder setRetryMaxNum(Integer retryMaxNum) {
            this.retryMaxNum = retryMaxNum;
            return this;
        }

        public Builder setQueryTimeOut(Long queryTimeOut) {
            this.queryTimeOut = queryTimeOut;
            return this;
        }

        public BsqlRedisLookupOptionsEntity build() {
            return new BsqlRedisLookupOptionsEntity(
                    tableName,
                    tableType,
                    cacheSize,
                    cacheTimeout,
                    cacheType,
                    multikeyDelimiter,
                    url,
                    password,
                    username,
                    redisType,
                    multiKey,
                    keyIndex,
                    retryMaxNum,
                    queryTimeOut);
        }
    }
}
