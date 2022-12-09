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

import java.util.Date;
import java.util.Map;

final class KafkaMessage {
	// 指标名称，示例：flink_taskmanager_job_task_busyTimeMsPerSecond
	private String metricName;
	// 指标Label，示例："task_name":"Source:_Kafka_Source____Sink:_Print_to_Std__Out","task_attempt_num":"0"
	private Map<String, String> tags;
	// 任务名称
	private String saberJobName;
	// 任务id
	private String saberJobId;
	// 任务owner
	private String jobOwner;
	// 任务使用资源队列
	private String queue;
	// 指标值
	private String value;
	// 创建时间
	private Date creatTime;
	// 指标类型，Gauge,Counter,Meter,Histogram
	private String metricType;
	// 任务执行集群类型,K8s, Yarn
	private String clusterType;
	// 指标主机名
	private String instanceHostName;
	// 指标主IP
	private String instanceHostIp;
	// 任务执行的execution.target值
	private String executionTarget;

	public static final class KafkaMessageBuilder {
		// 指标名称，示例：flink_taskmanager_job_task_busyTimeMsPerSecond
		private String metricName;
		// 指标Label，示例："task_name":"Source:_Kafka_Source____Sink:_Print_to_Std__Out","task_attempt_num":"0"
		private Map<String, String> tags;
		// 任务名称
		private String saberJobName;
		// 任务id
		private String saberJobId;
		// 任务owner
		private String jobOwner;
		// 任务使用资源队列
		private String queue;
		// 指标值
		private String value;
		// 创建时间
		private Date creatTime;
		// 指标类型，Gauge,Counter,Meter,Histogram
		private String metricType;
		// 任务执行集群类型,K8s, Yarn
		private String clusterType;
		// 指标主机名
		private String instanceHostName;
		// 指标主IP
		private String instanceHostIp;
		// 任务执行的execution.target值
		private String executionTarget;

		private KafkaMessageBuilder() {
		}

		public static KafkaMessageBuilder create() {
			return new KafkaMessageBuilder();
		}

		public KafkaMessageBuilder withMetricName(String metricName) {
			this.metricName = metricName;
			return this;
		}

		public KafkaMessageBuilder withTags(Map<String, String> tags) {
			this.tags = tags;
			return this;
		}

		public KafkaMessageBuilder withSaberJobName(String saberJobName) {
			this.saberJobName = saberJobName;
			return this;
		}

		public KafkaMessageBuilder withSaberJobId(String saberJobId) {
			this.saberJobId = saberJobId;
			return this;
		}

		public KafkaMessageBuilder withJobOwner(String jobOwner) {
			this.jobOwner = jobOwner;
			return this;
		}

		public KafkaMessageBuilder withQueue(String queue) {
			this.queue = queue;
			return this;
		}

		public KafkaMessageBuilder withValue(String value) {
			this.value = value;
			return this;
		}

		public KafkaMessageBuilder withCreatTime(Date creatTime) {
			this.creatTime = creatTime;
			return this;
		}

		public KafkaMessageBuilder withMetricType(String metricType) {
			this.metricType = metricType;
			return this;
		}

		public KafkaMessageBuilder withClusterType(String clusterType) {
			this.clusterType = clusterType;
			return this;
		}

		public KafkaMessageBuilder withInstanceHostName(String instanceHostName) {
			this.instanceHostName = instanceHostName;
			return this;
		}

		public KafkaMessageBuilder withInstanceHostIp(String instanceHostIp) {
			this.instanceHostIp = instanceHostIp;
			return this;
		}

		public KafkaMessageBuilder withExecutionTarget(String executionTarget) {
			this.executionTarget = executionTarget;
			return this;
		}

		public KafkaMessage build() {
			KafkaMessage kafkaMessage = new KafkaMessage();
			kafkaMessage.saberJobName = this.saberJobName;
			kafkaMessage.queue = this.queue;
			kafkaMessage.metricType = this.metricType;
			kafkaMessage.creatTime = this.creatTime;
			kafkaMessage.clusterType = this.clusterType;
			kafkaMessage.value = this.value;
			kafkaMessage.instanceHostName = this.instanceHostName;
			kafkaMessage.tags = this.tags;
			kafkaMessage.saberJobId = this.saberJobId;
			kafkaMessage.metricName = this.metricName;
			kafkaMessage.instanceHostIp = this.instanceHostIp;
			kafkaMessage.jobOwner = this.jobOwner;
			kafkaMessage.executionTarget = this.executionTarget;
			return kafkaMessage;
		}
	}

	public String getMetricName() {
		return metricName;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public String getSaberJobName() {
		return saberJobName;
	}

	public String getSaberJobId() {
		return saberJobId;
	}

	public String getJobOwner() {
		return jobOwner;
	}

	public String getQueue() {
		return queue;
	}

	public String getValue() {
		return value;
	}

	public Date getCreatTime() {
		return creatTime;
	}

	public String getMetricType() {
		return metricType;
	}

	public String getClusterType() {
		return clusterType;
	}

	public String getInstanceHostName() {
		return instanceHostName;
	}

	public String getInstanceHostIp() {
		return instanceHostIp;
	}

	public String getExecutionTarget() {
		return executionTarget;
	}
}
