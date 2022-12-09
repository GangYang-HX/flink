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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.listener.BacklogEventChangedListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to encapsulate the logic of building a {@link RecordWriter} instance.
 */
public class RecordWriterBuilder<T extends IOReadableWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(RecordWriterBuilder.class);

	private ChannelSelector<T> selector = new RoundRobinChannelSelector<>();

	private long timeout = -1;

	private String taskName = "test";

	private ExecutionConfig executionConfig;

	public RecordWriterBuilder<T> setChannelSelector(ChannelSelector<T> selector) {
		this.selector = selector;
		return this;
	}

	public RecordWriterBuilder<T> setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public RecordWriterBuilder<T> setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public RecordWriterBuilder<T> setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
		return this;
	}

	public RecordWriter<T> build(ResultPartitionWriter writer) {
		if (selector.isBroadcast()) {
			return new BroadcastRecordWriter<>(writer, timeout, taskName);
		} else {
			if (selector.isLoadBasedChannelSelectorEnabled()) {
				selector = getLoadBasedChannelSelector(writer);
			}
			return new ChannelSelectorRecordWriter<>(writer, selector, timeout, taskName);
		}
	}

	private LoadBasedChannelSelector<T> getLoadBasedChannelSelector(ResultPartitionWriter writer) {
		LoadBasedStrategy loadBasedStrategy = getLoadBasedStrategy();
		BacklogEventChangedListener backlogEventChangedListener = new BacklogEventChangedListener(loadBasedStrategy);
		int numberOfSubpartitions = writer.getNumberOfSubpartitions();
		for (int i = 0; i < numberOfSubpartitions; i++) {
			ResultSubpartition subpartition = writer.getSubpartition(i);
			if (subpartition instanceof PipelinedSubpartition) {
				((PipelinedSubpartition)subpartition).registerListener(backlogEventChangedListener);
			}
		}
		LOG.info("Enable loadBasedChannelSelector with loadBasedStrategy: {} for task: {}",
			loadBasedStrategy, taskName);

		return new LoadBasedChannelSelector<>(loadBasedStrategy);
	}

	private LoadBasedStrategy getLoadBasedStrategy() {
		if ("threshold".equals(executionConfig.getLoadBasedChannelSelectorStrategy())) {
			return new ThresholdBacklogLoadBasedStrategy(
				executionConfig.getChannelSelectorStrategyThresholdFactor(),
				executionConfig.getChannelSelectorStrategyThresholdUpdateFrequencyCount());
		}
		// Default use MinBacklogLoadBasedStrategy.
		return new MinBacklogLoadBasedStrategy(executionConfig.getChannelSelectorStrategyMinUpdateInterval());
	}
}
