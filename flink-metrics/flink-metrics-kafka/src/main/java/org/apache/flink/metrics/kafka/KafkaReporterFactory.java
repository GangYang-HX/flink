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

import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.util.StringUtils;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.metrics.kafka.KafkaReporterOptions.ACKS_CONFIG;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.BATCH_SIZE_CONFIG;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.BUFFER_MEMORY_CONFIG;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.FULL_LABELS;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.LABELS;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.LINGER_MS_CONFIG;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.REQUEST_TIMEOUT_MS;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.RETRIES_CONFIG;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.RETRY_BACKOFF_MS;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.TOPIC_NAME;

/**
 * {@link MetricReporterFactory} for {@link KafkaReporter}.
 */
@InterceptInstantiationViaReflection(
	reporterClassName = "org.apache.flink.metrics.kafka.KafkaReporter")
public class KafkaReporterFactory implements MetricReporterFactory {

	private static final Logger log = LoggerFactory.getLogger(KafkaReporterFactory.class);

	// labels属性分割符
	private final String labelsSeparator = ";";

	// attribute字段分割符
	private final String attributeSeparator = "=";

	@Override
	public MetricReporter createMetricReporter(Properties properties) {
		MetricConfig metricConfig = (MetricConfig) properties;

		String labels = metricConfig.getString(LABELS.key(), LABELS.defaultValue());
		if (StringUtils.isNullOrWhitespaceOnly(labels)) {
			labels = metricConfig.getString(FULL_LABELS.key(), FULL_LABELS.defaultValue());
		}

		// 初始化kafka producer 属性
		String topicName = metricConfig.getString(TOPIC_NAME.key(), TOPIC_NAME.defaultValue());

		String bootstrapServersConfig =
			metricConfig.getString(
				BOOTSTRAP_SERVERS_CONFIG.key(), BOOTSTRAP_SERVERS_CONFIG.defaultValue());

		if (StringUtils.isNullOrWhitespaceOnly(topicName)
			|| StringUtils.isNullOrWhitespaceOnly(bootstrapServersConfig)) {
			log.warn(
				"Invalid topicName {topicName:{}, bootstrapServersConfig:{}} must not be empty",
				topicName,
				bootstrapServersConfig);
		}

		String acks = metricConfig.getString(ACKS_CONFIG.key(), ACKS_CONFIG.defaultValue());

		Integer maxInFlightRequestsPerConnection =
			metricConfig.getInteger(
				MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.key(),
				MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.defaultValue());

		Integer retries =
			metricConfig.getInteger(RETRIES_CONFIG.key(), RETRIES_CONFIG.defaultValue());

		Integer batchSize =
			metricConfig.getInteger(BATCH_SIZE_CONFIG.key(), BATCH_SIZE_CONFIG.defaultValue());

		Integer linerMs =
			metricConfig.getInteger(LINGER_MS_CONFIG.key(), LINGER_MS_CONFIG.defaultValue());

		Integer bufferMemory =
			metricConfig.getInteger(
				BUFFER_MEMORY_CONFIG.key(), BUFFER_MEMORY_CONFIG.defaultValue());

		Integer requestTimeoutMs =
			metricConfig.getInteger(
				REQUEST_TIMEOUT_MS.key(), REQUEST_TIMEOUT_MS.defaultValue());

		Integer retryBackoffMs =
			metricConfig.getInteger(RETRY_BACKOFF_MS.key(), RETRY_BACKOFF_MS.defaultValue());

		Properties props = new Properties();
		props.put(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
			bootstrapServersConfig); // kafka集群，broker-list
		props.put(ProducerConfig.ACKS_CONFIG, acks);
		props.put(ProducerConfig.RETRIES_CONFIG, retries);
		props.put(
			ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
			maxInFlightRequestsPerConnection);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // 批次大小
		props.put(ProducerConfig.LINGER_MS_CONFIG, linerMs); // 等待时间
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory); // RecordAccumulator缓冲区大小
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs); //
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs); // RecordAccumulator缓冲区大小

		props.put(
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			"org.apache.kafka.common.serialization.StringSerializer");
		props.put(
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			"org.apache.kafka.common.serialization.StringSerializer");


		Map<String, String> paramsMap = parseExtensionLabels(labels);

		String executionTarget = metricConfig.getString(DeploymentOptions.CLUSTER_TYPE.key(), "");
		paramsMap.put(DeploymentOptions.CLUSTER_TYPE.key(), executionTarget);

		String kubernetesClusterId =
			metricConfig.getString(DeploymentOptions.KUBERNETES_CLUSTER_ID.key(), "");
		paramsMap.put(DeploymentOptions.KUBERNETES_CLUSTER_ID.key(), kubernetesClusterId);

		String yarnApplicationQueue =
			metricConfig.getString(DeploymentOptions.YARN_APPLICATION_QUEUE.key(), "");
		paramsMap.put(DeploymentOptions.YARN_APPLICATION_QUEUE.key(), yarnApplicationQueue);

		String customCallerContextJobId =
			metricConfig.getString(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID.key(), "");
		paramsMap.put(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID.key(), customCallerContextJobId);


		String instanceHostName = "";
		String instanceHostIp = "";
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			instanceHostName = inetAddress.getHostName();
			instanceHostIp = inetAddress.getHostAddress();
		} catch (UnknownHostException e) {
			log.error("init inetAddress object error");
		}

		paramsMap.put(KafkaReporterConstant.INSTANCE_HOST_NAME, instanceHostName);
		paramsMap.put(KafkaReporterConstant.INSTANCE_HOST_IP, instanceHostIp);

		log.info(
			"Configured kafkaReporter with {" +
				"labels:{}, " +
				"topicName:{}," +
				" bootstrapServersConfig:{}," +
				" acks:{}, " +
				"maxInFlightRequestsPerConnection:{}, " +
				"retries:{}," +
				" batchSize:{}," +
				" linerMs:{}, " +
				"bufferMemory:{}," +
				"requestTimeoutMs:{}," +
				"retryBackoffMs:{}, " +
				"executionTarget:{}, " +
				"kubernetesClusterId:{}}, " +
				"yarnApplicationQueue:{} , " +
				"customCallerContextJobId: {}",
			labels,
			topicName,
			bootstrapServersConfig,
			acks,
			maxInFlightRequestsPerConnection,
			retries,
			batchSize,
			linerMs,
			bufferMemory,
			requestTimeoutMs,
			retryBackoffMs,
			executionTarget,
			kubernetesClusterId,
			yarnApplicationQueue,
			customCallerContextJobId);

		paramsMap.forEach((k, v) -> {
			log.info(" paramsMap -> key:{}, value:{} ", k, v);
		});

		return new KafkaReporter(topicName, props, paramsMap);
	}

	// 获取自定义扩展的任务信息：saber_job_name=xxx;job_owner=xxx;queue_name=xxx
	private Map<String, String> parseExtensionLabels(final String labels) {
		Map<String, String> extensionJobInfo = new HashMap<>();
		if (labels == null || labels.isEmpty()) {
			return extensionJobInfo;
		}

		String[] kvs = labels.split(labelsSeparator);
		for (String kv : kvs) {
			int idx = kv.indexOf(attributeSeparator);
			if (idx < 0) {
				log.warn("Invalid label: {}, will be ignored", kv);
				continue;
			}

			String labelKey = kv.substring(0, idx);
			String labelValue = kv.substring(idx + 1);
			if (StringUtils.isNullOrWhitespaceOnly(labelKey)
				|| StringUtils.isNullOrWhitespaceOnly(labelValue)) {
				log.warn(
					"Invalid label {labelKey:{}, labelValue:{}} must not be empty",
					labelKey,
					labelValue);
				continue;
			}
			extensionJobInfo.put(labelKey, labelValue);
		}

		return extensionJobInfo;
	}


}
