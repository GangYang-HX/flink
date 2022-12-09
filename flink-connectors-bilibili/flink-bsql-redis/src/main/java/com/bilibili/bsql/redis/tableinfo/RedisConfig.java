/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.bilibili.bsql.redis.RedisConstant;

/**
 * @author zhouxiaogang
 * @version $Id: RedisConfig.java, v 0.1 2020-10-13 19:21
 * zhouxiaogang Exp $$
 */
public class RedisConfig {


	public static final String URL_KEY = "url";
	public static final String PASSWORD_KEY = "password";
	public static final String REDIS_TYPE = "redistype";
	public static final String RETRY_MAX_NUM = "retryMaxNum";
	public static final String QUERY_TIME_OUT = "queryTimeOut";

	public static final ConfigOption<String> BSQL_REDIS_URL = ConfigOptions
		.key(URL_KEY)
		.stringType()
		.noDefaultValue()
		.withDescription("redis url");

	public static final ConfigOption<String> BSQL_REDIS_PSWD = ConfigOptions
		.key(PASSWORD_KEY)
		.stringType()
		.defaultValue("")
		.withDescription("redis password");

	public static final ConfigOption<Integer> BSQL_REDIS_TYPE = ConfigOptions
		.key(REDIS_TYPE)
		.intType()
		.defaultValue(RedisConstant.CLUSTER)
		.withDescription("redis type");

	public static final ConfigOption<Integer> BSQL_RETRY_MAX_NUM = ConfigOptions
		.key(RETRY_MAX_NUM)
		.intType()
		.defaultValue(3)
		.withDescription("retry max num");

	public static final ConfigOption<Long> BSQL_QUERY_TIME_OUT = ConfigOptions
		.key(QUERY_TIME_OUT)
		.longType()
		.defaultValue(60000L)
		.withDescription("query time out ");
}
