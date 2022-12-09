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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collects metrics of a result sub partition.
 */
public class ResultSubPartitionMetrics {

	private final ResultSubpartition subPartition;

	private static final String METRIC_OUTPUT_BACKLOG = "outBacklog";

	// ------------------------------------------------------------------------

	private ResultSubPartitionMetrics(ResultSubpartition subPartition) {
		this.subPartition = checkNotNull(subPartition);
	}

	/**
	 * Get the backlog of sub partition.
	 *
	 * @return the number of backlog
	 */
	int getBacklog() {
		if (subPartition instanceof PipelinedSubpartition) {
			return ((PipelinedSubpartition)subPartition).getBuffersInBacklog();
		}
		return 0;
	}

	private Gauge<Integer> getBacklogGauge() {
		return new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return getBacklog();
			}
		};
	}

	public static void registerBacklogMetrics(MetricGroup parent, ResultSubpartition[] subpartitions) {
		for (int j = 0; j < subpartitions.length; j++) {
			MetricGroup group = parent.addGroup("sub_partition_index", Integer.toString(j), QueryServiceMode.DISABLED);
			ResultSubpartition subPartition = subpartitions[j];
			ResultSubPartitionMetrics metric = new ResultSubPartitionMetrics(subPartition);
			group.gauge(METRIC_OUTPUT_BACKLOG, metric.getBacklogGauge());
		}
	}
}
