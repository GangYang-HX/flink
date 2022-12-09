/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 *
 * @author zhouxiaogang
 * @version $Id: LatencyConfiguration.java, v 0.1 2020-11-23 17:29
zhouxiaogang Exp $$
 */
public class GlobalConfiguration {
	public static final String LATENCY_REDIS = "latency.redis";
	public static final String LATENCY_CLASS = "latency.class";
	public static final String LATENCY_REDIS_TYPE = "latency.redis.type";
	public static final String LATENCY_COMPRESS_TYPE = "latency.compress.type";
	public static final String LATENCY_REDIS_BUCKET_SIZE = "latency.redis.bucket.size";
	public static final String LATENCY_MAX_CACHE_SIZE = "latency.max.cache.size";
	public static final String LATENCY_MERGE_FUNCTION = "latency.merge.function";
	public static final String LATENCY_MERGE_DELIMITER = "latency.merge.delimiter";
	public static final String LATENCY_SAVE_KEY = "latency.save.key";

	public static final ConfigOption<String> LATENCY_REDIS_CONFIG = ConfigOptions
		.key(LATENCY_REDIS)
		.stringType()
		.noDefaultValue()
		.withDescription("latency join redis");

	public static final ConfigOption<String> LATENCY_CLASS_CONFIG = ConfigOptions
		.key(LATENCY_CLASS)
		.stringType()
		.noDefaultValue()
		.withDescription("latency join class");

	public static final ConfigOption<Integer> LATENCY_REDIS_TYPE_CONFIG = ConfigOptions
		.key(LATENCY_REDIS_TYPE)
		.intType()
		.defaultValue(1) //default type is 1
		.withDescription("latency join redis type");

	public static final ConfigOption<Integer> LATENCY_COMPRESS_TYPE_CONFIG = ConfigOptions
		.key(LATENCY_COMPRESS_TYPE)
		.intType()
		.defaultValue(3) //default type is 3
		.withDescription("latency join compress type");

	public static final ConfigOption<Integer> LATENCY_REDIS_BUCKET_SIZE_CONFIG = ConfigOptions
		.key(LATENCY_REDIS_BUCKET_SIZE)
		.intType()
		.defaultValue(4000) //default type is 4000
		.withDescription("latency join bucket size");

	public static final ConfigOption<Integer> LATENCY_MAX_CACHE_SIZE_CONFIG = ConfigOptions
		.key(LATENCY_MAX_CACHE_SIZE)
		.intType()
		.defaultValue(512000) //default type is 500k
		.withDescription("latency join max cache size");

	public static final ConfigOption<String> LATENCY_MERGE_FUNCTION_CONFIG = ConfigOptions
		.key(LATENCY_MERGE_FUNCTION)
		.stringType()
		.defaultValue("plain") //no default value
		.withDescription("latency join function type");

	public static final ConfigOption<Boolean> LATENCY_SAVE_KEY_CONFIG = ConfigOptions
		.key(LATENCY_SAVE_KEY)
		.booleanType()
		.defaultValue(false) //no default value
		.withDescription("latency save key");
}
