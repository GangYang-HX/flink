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

package org.apache.flink.metrics.kafka;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.LinkElement;
import org.apache.flink.configuration.description.TextElement;

/**
 * Config options for the {@link KafkaReporter}.
 */
@Documentation.SuffixOption
public class KafkaReporterOptions {

	public static final ConfigOption<String> METRIC_FILTER = ConfigOptions
		.key("metricFilter")
		.defaultValue("")
		.withDescription("Metric filter.");

	public static final ConfigOption<String> LABEL_FILTER = ConfigOptions
		.key("labelFilter")
		.defaultValue("")
		.withDescription("Label filter.");

	public static final ConfigOption<String> TOPIC_NAME =
		ConfigOptions.key("topicName")
			.noDefaultValue()
			.withDescription("The kafka topic name.");

	public static final ConfigOption<String> BOOTSTRAP_SERVERS_CONFIG =
		ConfigOptions.key("bootstrapServersConfig")
			.noDefaultValue()
			.withDescription("The kafka bootstrap server.");

	public static final ConfigOption<String> ACKS_CONFIG =
		ConfigOptions.key("acks").defaultValue("1").withDescription("The kafka acks.");

	public static final ConfigOption<Integer> MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION =
		ConfigOptions.key("maxInFlightRequestsPerConnection")
			.defaultValue(5)
			.withDescription("The kafka MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.");

	public static final ConfigOption<Integer> RETRIES_CONFIG =
		ConfigOptions.key("retries").defaultValue(5).withDescription("The kafka retries.");

	public static final ConfigOption<Integer> BATCH_SIZE_CONFIG =
		ConfigOptions.key("batchSize")
			.defaultValue(16384)
			.withDescription("The kafka BATCH_SIZE_CONFIG .");

	public static final ConfigOption<Integer> LINGER_MS_CONFIG =
		ConfigOptions.key("linerMs")
			.defaultValue(500)
			.withDescription("The kafka LINGER_MS_CONFIG .");

	public static final ConfigOption<Integer> BUFFER_MEMORY_CONFIG =
		ConfigOptions.key("bufferMemory")
			.defaultValue(33554432)
			.withDescription("The kafka LINGER_MS_CONFIG .");

	public static final ConfigOption<Integer> REQUEST_TIMEOUT_MS =
		ConfigOptions.key("requestTimeoutMs")
			.defaultValue(30000)
			.withDescription("The kafka requestTimeoutMs .");

	public static final ConfigOption<Integer> RETRY_BACKOFF_MS =
		ConfigOptions.key("retryBackoffMs")
			.defaultValue(1000)
			.withDescription("The kafka retryBackoffMs .");

	public static final ConfigOption<String> LABELS =
		ConfigOptions.key("labels").defaultValue("").withDescription("The job extension info");

	public static final ConfigOption<String> CLUSTER_TYPE =
		ConfigOptions.key("cluster_type").defaultValue("").withDescription("job execute cluster type");

	public static final ConfigOption<String> FULL_LABELS =
		ConfigOptions.key(ConfigConstants.METRICS_REPORTER_PREFIX + "kafkaReporter.labels").defaultValue("").withDescription("The job extension info");

	public static final ConfigOption<Integer> MAX_LABEL_VALUE_LENGTH =
		ConfigOptions.key("maxLabelValueLen")
			.defaultValue(100)
			.withDescription("Specifies the max label value.");

	public static final ConfigOption<Boolean> FILTER_LABEL_VALUE_CHARACTER =
		ConfigOptions.key("filterLabelValueCharacters")
			.defaultValue(true)
			.withDescription(
				Description.builder()
					.text(
						"Specifies whether to filter label value characters."
							+ " If enabled, all characters not matching [a-zA-Z0-9:_] will be removed,"
							+ " otherwise no characters will be removed."
							+ " Before disabling this option please ensure that your"
							+ " label values meet the %s.",
						LinkElement.link(
							"https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels",
							"Prometheus requirements"))
					.build());

	public static final ConfigOption<String> GROUPING_KEY =
		ConfigOptions.key("groupingKey")
			.defaultValue("")
			.withDescription(
				Description.builder()
					.text(
						"Specifies the grouping key which is the group and global labels of all metrics."
							+ " The label name and value are separated by '=', and labels are separated by ';', e.g., %s."
							+ " Please ensure that your grouping key meets the %s.",
						TextElement.code("k1=v1;k2=v2"),
						LinkElement.link(
							"https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels",
							"Prometheus requirements"))
					.build());
}
