/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.time.Duration;

/**
 * {@link ConfigOption}s specific for a single execution of a user program.
 */
@PublicEvolving
public class ExecutionOptions {

	/**
	 * Should be moved to {@code ExecutionCheckpointingOptions} along with
	 * {@code ExecutionConfig#useSnapshotCompression}, which should be put into {@code CheckpointConfig}.
	 */
	public static final ConfigOption<Boolean> SNAPSHOT_COMPRESSION =
		ConfigOptions.key("execution.checkpointing.snapshot-compression")
			.booleanType()
			.defaultValue(false)
		.withDescription("Tells if we should use compression for the state snapshot data or not");

	public static final ConfigOption<Duration> BUFFER_TIMEOUT =
		ConfigOptions.key("execution.buffer-timeout")
			.durationType()
			.defaultValue(Duration.ofMillis(100))
			.withDescription(Description.builder()
				.text("The maximum time frequency (milliseconds) for the flushing of the output buffers. By default " +
					"the output buffers flush frequently to provide low latency and to aid smooth developer " +
					"experience. Setting the parameter can result in three logical modes:")
				.list(
					TextElement.text("A positive value triggers flushing periodically by that interval"),
					TextElement.text("0 triggers flushing after every record thus minimizing latency"),
					TextElement.text("-1 ms triggers flushing only when the output buffer is full thus maximizing " +
						"throughput")
				)
				.build());

	public static final ConfigOption<String> CALLER_CONTEXT_APP_ID =
		ConfigOptions.key("execution.caller-context-app-id")
			.stringType()
			.defaultValue("")
			.withDescription(
				"A text means the caller context app id."
			);

	public static final ConfigOption<String> CUSTOM_CALLER_CONTEXT_JOB_ID =
		ConfigOptions.key("execution.custom.caller-context-job-id")
			.stringType()
			.noDefaultValue()
			.withDescription("user custom job id.");


	public static final ConfigOption<Boolean> LOAD_BASED_CHANNEL_SELECTOR_ENABLED =
		ConfigOptions.key("execution.load-based-channel-selector.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Enable load based strategy to distribute data, " +
				"when partitioner is RescalePartitioner or RebalancePartitioner.");

	public static final ConfigOption<String> LOAD_BASED_CHANNEL_SELECTOR_STRATEGY =
		ConfigOptions.key("execution.load-based-channel-selector.strategy")
			.stringType()
			.defaultValue("min")
			.withDescription("The strategy of load-based-channel-selector. Default is min " +
				"for MinBacklogLoadBasedStrategy, otherwise threshold for ThresholdBacklogLoadBasedStrategy. " +
				"It works when enable the config execution.load-based-channel-selector.enable.");

	public static final ConfigOption<Double> CHANNEL_SELECTOR_STRATEGY_THRESHOLD_FACTOR =
		ConfigOptions.key("execution.load-based-channel-selector.strategy.threshold.factor")
			.doubleType()
			.defaultValue(1.3)
			.withDescription("The factor of threshold. threshold = factor * average of backlogs. " +
				"It works when use threshold strategy.");

	public static final ConfigOption<Integer> CHANNEL_SELECTOR_STRATEGY_THRESHOLD_UPDATE_FREQUENCY_COUNT =
		ConfigOptions.key("execution.load-based-channel-selector.strategy.threshold.update-frequency-count")
			.intType()
			.defaultValue(50)
			.withDescription("The count of update threshold frequency. " +
				"It works when use threshold strategy.");

	public static final ConfigOption<Integer> CHANNEL_SELECTOR_STRATEGY_MIN_UPDATE_INTERVAL =
		ConfigOptions.key("execution.load-based-channel-selector.strategy.min.update-interval")
			.intType()
			.defaultValue(0)
			.withDescription("The interval of update load based strategy. " +
				"It works when the config execution.load-based-channel-selector.strategy is min.");

	public static final ConfigOption<Boolean> BACKLOG_METRICS_ENABLED =
		ConfigOptions.key("execution.backlog-metrics.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether use backlog metric or not.");


	public static final ConfigOption<Long> SPECULATIVE_SCAN_INTERVAL =
		ConfigOptions.key("execution.speculative.scan-interval.ms")
			.longType()
			.defaultValue(3000L)
			.withDescription("The interval (in ms) for scaning tasks in speculative manager.");


	public static final ConfigOption<Long> SPECULATIVE_REPORT_BATCH_SIZE =
		ConfigOptions.key("execution.speculative.reporting-batch-size")
			.longType()
			.defaultValue(100L)
			.withDescription("The size of data cache for reporting time costs.");

	public static final ConfigOption<Long> SPECULATIVE_REPORTING_TIMEOUT =
		ConfigOptions.key("execution.speculative.reporting-timeout.ms")
			.longType()
			.defaultValue(5000L)
			.withDescription("The timeout (in ms) for reporting time costs to jobmanager with rpc.");

	public static final ConfigOption<Integer> SPECULATIVE_THREAD_POOL_SIZE =
		ConfigOptions.key("execution.speculative.thread-pool-size")
			.intType()
			.defaultValue(5)
			.withDescription("The core thread pool size of the scheduler that is used to execute " +
					"the internal operations.");

}
