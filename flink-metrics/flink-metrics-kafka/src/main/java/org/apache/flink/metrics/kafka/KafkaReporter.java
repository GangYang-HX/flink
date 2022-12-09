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
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via KafkaReport.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter extends AbstractKafkaReporter implements Scheduled {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final String saberJobNameKey = "saber_job_name";
	private final String saberJobIDKey = "SABER_JOB_ID";
	private final String jobOwnerKey = "job_owner";
	private final String queueNameKey = "queue_name";


	private static final List<Double> quantiles = Arrays.asList(.5, .75, .95, .98, .99, .999);

	private static final List<String> histogramDimensions =
		Arrays.asList("count", "min", "max", "mean", "stddev");

	private final Producer<String, String> producer;
	private final String topicName;
	private final String saberJobName;
	private final String saberJobId;
	private final String jobOwner;
	private final String queueName;
	private final String executionTarget;
	private final String clusterType;
	private final String instanceHostName;
	private final String instanceHostIp;

	public KafkaReporter(String topicName, Properties props, Map<String, String> extensionJobInfo) {
		this.topicName = topicName;
		this.producer = new KafkaProducer<>(props);
		this.saberJobName = extensionJobInfo.get(saberJobNameKey);
		this.jobOwner = extensionJobInfo.get(jobOwnerKey);
		this.executionTarget = extensionJobInfo.get(DeploymentOptions.CLUSTER_TYPE.key());
		this.clusterType = getClusterType(executionTarget);

		// if job running on K8s queueName value is kubernetes.cluster-id
		if (clusterType.equals(KafkaReporterConstant.KUBERNETES_CLUSTER_TYPE)) {
			String kubernetesClusterId = extensionJobInfo.get(DeploymentOptions.KUBERNETES_CLUSTER_ID.key());
			this.queueName = kubernetesClusterId;
		} else {
			// if job running yarn-session mode
			if (executionTarget.equals(YarnDeploymentTarget.SESSION.getName())) {
				this.queueName = extensionJobInfo.get(DeploymentOptions.YARN_APPLICATION_QUEUE.key());
			} else {
				this.queueName = extensionJobInfo.get(queueNameKey);
			}
		}

		if (executionTarget.equals(YarnDeploymentTarget.SESSION.getName()) || executionTarget.equals(KubernetesDeploymentTarget.SESSION.getName())) {
			this.saberJobId = extensionJobInfo.get(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID.key());
		} else {
			this.saberJobId = System.getProperty(saberJobIDKey);
		}

		this.instanceHostName = extensionJobInfo.get(KafkaReporterConstant.INSTANCE_HOST_NAME);
		this.instanceHostIp = extensionJobInfo.get(KafkaReporterConstant.INSTANCE_HOST_IP);
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.flush();
			producer.close();
		}
	}

	@Override
	public void report() {
		if (StringUtils.isNullOrWhitespaceOnly(topicName)) {
			log.warn("Invalid topicName {topicName:{}} must not be empty", topicName);
			return;
		}
		log.debug("---KafkaReporter start report-------");

		long startTime = System.currentTimeMillis();
		Date nowDate = new Date();

		for (Map.Entry<Gauge<?>, KafkaMetricsInfo> entry : gauges.entrySet()) {
			String value = gaugeFrom(entry.getKey());
			sendMetricsToKafka(
				saberJobName,
				saberJobId,
				queueName,
				jobOwner,
				nowDate,
				entry.getValue().getName(),
				entry.getValue().getTags(),
				value,
				"Gauge",
				clusterType,
				instanceHostName,
				instanceHostIp,
				executionTarget);
		}

		for (Map.Entry<Counter, KafkaMetricsInfo> entry : counters.entrySet()) {
			String value = gaugeFrom(entry.getKey());
			sendMetricsToKafka(
				saberJobName,
				saberJobId,
				queueName,
				jobOwner,
				nowDate,
				entry.getValue().getName(),
				entry.getValue().getTags(),
				value,
				"Counter",
				clusterType,
				instanceHostName,
				instanceHostIp,
				executionTarget);
		}

		for (Map.Entry<Meter, KafkaMetricsInfo> entry : meters.entrySet()) {
			String value = gaugeFrom(entry.getKey());
			sendMetricsToKafka(
				saberJobName,
				saberJobId,
				queueName,
				jobOwner,
				nowDate,
				entry.getValue().getName(),
				entry.getValue().getTags(),
				value,
				"Meter",
				clusterType,
				instanceHostName,
				instanceHostIp,
				executionTarget);
		}

		for (Map.Entry<Histogram, KafkaMetricsInfo> entry : histograms.entrySet()) {
			handleHistogramMetrics(
				saberJobName,
				saberJobId,
				queueName,
				jobOwner,
				nowDate,
				entry.getKey(),
				entry.getValue(),
				clusterType,
				instanceHostName,
				instanceHostIp,
				executionTarget);
		}

		long castDown = System.currentTimeMillis() - startTime;
		log.debug("---KafkaReporter end report-------共耗时:{} 毫秒", castDown);
	}

	// 处理Histogram类型，metrics数据
	private void handleHistogramMetrics(
		final String saberJobName,
		final String saberJobId,
		final String queueName,
		final String jobOwner,
		final Date nowDate,
		final Histogram histogram,
		final KafkaMetricsInfo kafkaMetricsInfo,
		final String clusterType,
		final String instanceHostName,
		final String instanceHostIp,
		final String executionTarget) {
		String metricType = "Histogram";
		HistogramStatistics statistics = histogram.getStatistics();
		String metricName = kafkaMetricsInfo.getName();
		Map<String, String> tags = kafkaMetricsInfo.getTags();

		for (final String histogramDimension : histogramDimensions) {
			String value;
			switch (histogramDimension) {
				case "count":
					value = String.valueOf((statistics.size()));
					break;
				case "min":
					value = String.valueOf((statistics.getMin()));
					break;
				case "max":
					value = String.valueOf((statistics.getMax()));
					break;
				case "mean":
					Double mean = statistics.getMean();
					value = doubleToString(mean);
					break;
				case "stddev":
					Double stdDev = statistics.getStdDev();
					value = doubleToString(stdDev);
					break;
				default:
					value = "";
			}

			sendMetricsToKafka(
				saberJobName,
				saberJobId,
				queueName,
				jobOwner,
				nowDate,
				metricName + "_" + histogramDimension,
				tags,
				value,
				metricType,
				clusterType,
				instanceHostName,
				instanceHostIp,
				executionTarget);
		}

		for (final Double quantile : quantiles) {
			Double value = statistics.getQuantile(quantile);
			HashMap<String, String> labels = new HashMap<>(tags);
			labels.put("quantile", quantile.toString());
			sendMetricsToKafka(
				saberJobName,
				saberJobId,
				queueName,
				jobOwner,
				nowDate,
				metricName,
				labels,
				doubleToString(value),
				metricType,
				clusterType,
				instanceHostName,
				instanceHostIp,
				executionTarget);
		}
	}

	// 组装metrics数据，发送到kafka
	private void sendMetricsToKafka(
		String saberJobName,
		String saberJobId,
		String queueName,
		String jobOwner,
		Date nowDate,
		String metricName,
		Map<String, String> tags,
		String value,
		String metricType,
		String clusterType,
		String instanceHostName,
		String instanceHostIp,
		String executionTarget) {

		String key = saberJobId;
		if (StringUtils.isNullOrWhitespaceOnly(key)) {
			key = metricName;
		}

		KafkaMessage kafkaMessage =
			KafkaMessage.KafkaMessageBuilder.create()
				.withSaberJobName(saberJobName)
				.withSaberJobId(saberJobId)
				.withQueue(queueName)
				.withJobOwner(jobOwner)
				.withCreatTime(nowDate)
				.withMetricName(metricName)
				.withTags(tags)
				.withValue(value)
				.withMetricType(metricType)
				.withClusterType(clusterType)
				.withInstanceHostName(instanceHostName)
				.withInstanceHostIp(instanceHostIp)
				.withExecutionTarget(executionTarget)
				.build();
		try {
			String writeValueAsString = objectMapper.writeValueAsString(kafkaMessage);
			producer.send(
				new ProducerRecord<String, String>(
					topicName.trim(), key, writeValueAsString),
				(metadata, exception) -> {
					if (exception != null) {
						log.error(
							"flink metrics send to kafka error, topic={}, key={}, message={}, e={}",
							topicName,
							metricName,
							writeValueAsString,
							exception);
					}
				});
		} catch (Exception e) {
			log.error("send flink metrics message to kafka error...e={}", e);
		}
	}

	private String gaugeFrom(Gauge gauge) {
		final Object value = gauge.getValue();

		if (value == null) {
			log.warn("Gauge {} is null-valued, defaulting to 0.", gauge);
			return "0";
		}

		if (value instanceof Double) {
			return doubleToString((Double) value);
		}

		if (value instanceof Number) {
			return doubleToString(((Number) value).doubleValue());
		}

		if (value instanceof Boolean) {
			return String.valueOf(value);
		}

		if (value instanceof String) {
			return String.valueOf(value);
		}

		log.warn(
			"Invalid type for Gauge {}: {}, only number types and booleans and String are supported by this reporter.",
			gauge,
			value.getClass().getName());
		return "0";
	}

	private String gaugeFrom(Counter counter) {
		return String.valueOf(counter.getCount());
	}

	private String gaugeFrom(Meter meter) {
		return doubleToString(meter.getRate());
	}

	private String doubleToString(Double value) {
		String returnValue = "";
		try {
			BigDecimal bigDecimal = new BigDecimal(String.valueOf(value));
			returnValue = bigDecimal.toPlainString();
		} catch (Exception e) {
			returnValue = String.valueOf(value);
		}
		return returnValue;
	}

	private String getClusterType(String executionTarget) {
		if (executionTarget.equals(YarnDeploymentTarget.APPLICATION.getName())
			|| executionTarget.equals(YarnDeploymentTarget.PER_JOB.getName())
			|| executionTarget.equals(YarnDeploymentTarget.SESSION.getName())) {
			return KafkaReporterConstant.YARN_CLUSTER_TYPE;
		}

		if (executionTarget.equals(KubernetesDeploymentTarget.APPLICATION.getName())
			|| executionTarget.equals(KubernetesDeploymentTarget.SESSION.getName())) {
			return KafkaReporterConstant.KUBERNETES_CLUSTER_TYPE;
		}
		return KafkaReporterConstant.UNKNOWN_CLUSTER_TYPE;
	}
}
