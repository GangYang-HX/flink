/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/**
 * Gauge for getting the current value of a Kafka metric.
 */
@Internal
public class KafkaMetricWrapper implements Gauge<Double>, KafkaConsumerMetrics {

	private final org.apache.kafka.common.Metric kafkaMetric;
	Histogram pollsTime;
	Histogram pollIdleRatio;
	public static final int DEFAULT_WINDOW_SIZE = 10_000;
	private long lastPollMs;
	private double pollIdleRatioValue;
	private long pollTimeMs;

	public KafkaMetricWrapper(org.apache.kafka.common.Metric metric) {
		this.kafkaMetric = metric;
	}

	public KafkaMetricWrapper() {
		this.kafkaMetric = new Metric() {
			@Override
			public MetricName metricName() {
				return null;
			}

			@Override
			public double value() {
				return 0;
			}
		};

	}
	private final Object lock = new Object();


	@Override
	public Double getValue() {
		return kafkaMetric.value();
	}

	@Override
	public void pollsTimeUse(long start, long end) {
		synchronized (lock) {
			pollsTime.update(end - start);
		}
	}

	public KafkaMetricWrapper setCompactionTime(Histogram pollsTime) {
		this.pollsTime = pollsTime;
		return this;
	}

	public KafkaMetricWrapper setPollIdleRatio(Histogram pollIdleRatio) {
		this.pollIdleRatio = pollIdleRatio;
		return this;
	}

	public double getPollIdleRatio() {
		return pollIdleRatioValue;
	}

	@Override
	public void recordPollTimeMs(long pollStartMs, long pollEndMs) {
		this.pollTimeMs = pollEndMs - pollStartMs;
	}

	@Override
	public void recordPollRatio(long pollStart) {
		synchronized (lock) {
			long timeSinceLastPollMs = lastPollMs != 0L ? pollStart - lastPollMs : 0;
			this.pollIdleRatioValue = timeSinceLastPollMs == 0 ? 0 : pollTimeMs * 1.0 / timeSinceLastPollMs * 1.0;
			this.pollIdleRatio.update(new Double(this.pollIdleRatioValue * 1000).longValue());
			this.lastPollMs = pollStart;
		}
	}
}
