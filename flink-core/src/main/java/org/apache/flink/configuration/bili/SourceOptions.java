/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.configuration.bili;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to sources
 *
 */
public class SourceOptions {

	/**
	 * Timeout in milliseconds before the kafka source to toggle idle.
	 */
	public static final ConfigOption<Long> KAFKA_IDLE_INTERVAL =
		key("task.kafka_source_idle.interval")
			.defaultValue(30000L)
			.withDescription("Time interval to check milliseconds.");
}
