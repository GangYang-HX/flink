/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */

package org.apache.flink.table.runtime.operators.join.latency.v2.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * author: wangding01
 * date: 2022-02-23 18:29
 */
public class LatencyJoinConfiguration {
	public static final String JOIN_LATENCY_MERGE_ENABLE = "join.latency.merge.enable";
	public static final String JOIN_LATENCY_MERGE_DELIMITER = "join.latency.merge.delimiter";
	public static final String JOIN_LATENCY_DISTINCT = "join.latency.distinct";
	public static final String JOIN_LATENCY_TTL_ENABLE = "join.latency.ttl.enable";
	public static final String JOIN_LATENCY_CLEANUP_INTERVAL = "join.latency.cleanup";

	public static final ConfigOption<Boolean> JOIN_LATENCY_MERGE_ENABLE_CONFIG = ConfigOptions
			.key(JOIN_LATENCY_MERGE_ENABLE)
			.booleanType()
			.defaultValue(true) //todo default need set false
			.withDescription("latency join function type");

	public static final ConfigOption<String> JOIN_LATENCY_MERGE_DELIMITER_CONFIG = ConfigOptions
			.key(JOIN_LATENCY_MERGE_DELIMITER)
			.stringType()
			.defaultValue(",")
			.withDescription("latency join delimiter string");

	public static final ConfigOption<Boolean> JOIN_LATENCY_DISTINCT_CONFIG = ConfigOptions
			.key(JOIN_LATENCY_DISTINCT)
			.booleanType()
			.defaultValue(true)
			.withDescription("latency join distinct keys");

	public static final ConfigOption<Boolean> JOIN_LATENCY_TTL_ENABLE_CONFIG = ConfigOptions
			.key(JOIN_LATENCY_TTL_ENABLE)
			.booleanType()
			.defaultValue(false)
			.withDescription("latency join ttl enable keys");

	public static final ConfigOption<Integer> JOIN_LATENCY_CLEANUP_INTERVAL_CONFIG = ConfigOptions
		.key(JOIN_LATENCY_CLEANUP_INTERVAL)
		.intType()
		.defaultValue(-1)
		.withDescription("latency join cleanup interval")
		.withDeprecatedKeys("latency.cleanup");
}
