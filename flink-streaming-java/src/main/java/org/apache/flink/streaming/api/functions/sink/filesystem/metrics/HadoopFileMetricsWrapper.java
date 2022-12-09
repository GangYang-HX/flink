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

package org.apache.flink.streaming.api.functions.sink.filesystem.metrics;

import org.apache.flink.core.fs.MetricsWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

public class HadoopFileMetricsWrapper implements MetricsWrapper {

	private final Object lock = new Object();
	private Histogram openRt;
	private Histogram flushRt;
	private Histogram closeRt;

	public HadoopFileMetricsWrapper register(MetricGroup metricGroup) {
		MetricGroup hadoopGroup = metricGroup
			.addGroup(HadoopFileMetricsConstants.HADOOP_FILE_METRICS_GROUP, QueryServiceMode.DISABLED);
		int WINDOW_SIZE = 1000;
		this.openRt =
			hadoopGroup
				.histogram(HadoopFileMetricsConstants.OPEN_FILE_COST_TIME_METRICS,
					new DescriptiveStatisticsHistogram(WINDOW_SIZE));
		this.flushRt = hadoopGroup
			.histogram(HadoopFileMetricsConstants.FLUSH_FILE_COST_TIME_METRICS,
				new DescriptiveStatisticsHistogram(WINDOW_SIZE));
		this.closeRt = hadoopGroup
			.histogram(HadoopFileMetricsConstants.CLOSE_FILE_COST_TIME_METRICS
				, new DescriptiveStatisticsHistogram(WINDOW_SIZE));
		return this;
	}

	@Override
	public void recordOpenRt(long current) {
		long rt = System.nanoTime() - current;
		synchronized (lock) {
			openRt.update(rt);
		}
	}

	@Override
	public void recordFlushRt(long current) {
		long rt = System.nanoTime() - current;
		synchronized (lock) {
			flushRt.update(rt);
		}
	}

	@Override
	public void recordCloseRt(long current) {
		long rt = System.nanoTime() - current;
		synchronized (lock) {
			closeRt.update(rt);
		}
	}
}
