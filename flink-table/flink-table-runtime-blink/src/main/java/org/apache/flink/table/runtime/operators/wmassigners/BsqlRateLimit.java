/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RateLimit.java, v 0.1 2020-12-24 16:15
zhouxiaogang Exp $$
 */
public class BsqlRateLimit {
	public static final String RATE_LIMITER         = "rateLimiter";
	public static final String TOLERATE_INTERVAL    = "tolerateInterval";
	public static final String SCHEDULER_TIME       = "schedulerTime";
	public static final String ESTIMATE_TIME        = "estimateTime";
	public static final String TASK_SPEED_THRESHOLD = "taskSpeedThreshold";
	public static final String DEBUG_CONTROL_SPEED  = "debugControlSpeed";

	public static final ConfigOption<String> RATE_LIMITER_CONFIG = ConfigOptions
		.key(RATE_LIMITER)
		.stringType()
		.noDefaultValue()
		.withDescription("");

	public static final ConfigOption<String> TOLERATE_INTERVAL_CONFIG = ConfigOptions
		.key(TOLERATE_INTERVAL)
		.stringType()
		.noDefaultValue()
		.withDescription("");

	public static final ConfigOption<String> SCHEDULER_TIME_CONFIG = ConfigOptions
		.key(SCHEDULER_TIME)
		.stringType()
		.noDefaultValue()
		.withDescription("");

	public static final ConfigOption<String> ESTIMATE_TIME_CONFIG = ConfigOptions
		.key(ESTIMATE_TIME)
		.stringType()
		.noDefaultValue()
		.withDescription("");

	public static final ConfigOption<String> TASK_SPEED_THRESHOLD_CONFIG = ConfigOptions
		.key(TASK_SPEED_THRESHOLD)
		.stringType()
		.noDefaultValue()
		.withDescription("");

	public static final ConfigOption<String> DEBUG_CONTROL_SPEED_CONFIG = ConfigOptions
		.key(DEBUG_CONTROL_SPEED)
		.stringType()
		.noDefaultValue()
		.withDescription("");
}
