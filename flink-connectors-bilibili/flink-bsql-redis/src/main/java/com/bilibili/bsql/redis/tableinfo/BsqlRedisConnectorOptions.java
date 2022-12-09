/** Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved. */
package com.bilibili.bsql.redis.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.bilibili.bsql.redis.RedisConstant;

/** @version $Id: BsqlRedisConnectorOptions.java */
public class BsqlRedisConnectorOptions {

    public static final String REDIS_TYPE = "redistype";
    public static final String RETRY_MAX_NUM = "retryMaxNum";
    public static final String QUERY_TIME_OUT = "queryTimeOut";

    public static final ConfigOption<Integer> BSQL_REDIS_TYPE =
            ConfigOptions.key(REDIS_TYPE)
                    .intType()
                    .defaultValue(RedisConstant.CLUSTER)
                    .withDescription("redis type");

    public static final ConfigOption<Integer> BSQL_RETRY_MAX_NUM =
            ConfigOptions.key(RETRY_MAX_NUM)
                    .intType()
                    .defaultValue(3)
                    .withDescription("retry max num");

    public static final ConfigOption<Long> BSQL_QUERY_TIME_OUT =
            ConfigOptions.key(QUERY_TIME_OUT)
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("query time out ");
}
