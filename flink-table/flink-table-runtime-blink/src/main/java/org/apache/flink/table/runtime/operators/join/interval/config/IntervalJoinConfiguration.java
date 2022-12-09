/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */

package org.apache.flink.table.runtime.operators.join.interval.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author wangding01
 * @version $Id: IntervalConfiguration.java, v 0.1 2021-12-30 20:29
 */
public class IntervalJoinConfiguration {
	public static final String INTERVAL_CLEANUP_INTERVAL = "join.interval.cleanup";

	public static final ConfigOption<Integer> INTERVAL_CLEANUP_INTERVAL_CONFIG = ConfigOptions
			.key(INTERVAL_CLEANUP_INTERVAL)
			.intType()
			.defaultValue(-1)
			.withDescription("interval join cleanup interval")
			.withDeprecatedKeys("interval.cleanup");
}
