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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.listener.BacklogEvent;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.IndexedPriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a load based implementation of the {@link LoadBasedStrategy} interface based on minimum backlog. Generally,
 * it distributes the data to the least loaded channel. However, the round robin channel is selected when the load
 * is equal to the least loaded channel.
 */
public final class MinBacklogLoadBasedStrategy implements LoadBasedStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(MinBacklogLoadBasedStrategy.class);

	private static final int DEFAULT_UPDATE_INTERVAL = 0;

	private int numberOfChannels;

	/**
	 * Store the subPartition index and backlog order by backlog.
	 */
	private IndexedPriorityQueue<Integer> priorityQueue;

	/**
	 * Stores the index of the channel to send the next record to.
	 */
	private int nextChannelToSendTo = -1;

	private final int updateInterval;

	private boolean updatePeriodically;

	private ScheduledExecutorService backlogUpdater;

	private int[] backlogs;

	public MinBacklogLoadBasedStrategy() {
		this(DEFAULT_UPDATE_INTERVAL);
	}

	public MinBacklogLoadBasedStrategy(int updateInterval) {
		this.updateInterval = updateInterval;
		if (updateInterval > 0) {
			this.updatePeriodically = true;
			backlogUpdater = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("backlogUpdater"));
		}
	}

	@Override
	public void setup(int numberOfChannels) {
		this.numberOfChannels = numberOfChannels;

		this.priorityQueue = new IndexedPriorityQueue<>(numberOfChannels);
		for (int i = 0; i < numberOfChannels; i++) {
			priorityQueue.insert(i, 0);
		}
		if (updatePeriodically) {
			backlogs = new int[numberOfChannels];
			backlogUpdater.scheduleAtFixedRate(this::updateQueue, 500, updateInterval, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public int select(final IOReadableWritable record) {
		nextChannelToSendTo = nextChannel(nextChannelToSendTo, numberOfChannels);
		int nextBacklog = priorityQueue.get(nextChannelToSendTo);
		return priorityQueue.minValue() == nextBacklog ?
			   nextChannelToSendTo :
			   priorityQueue.minIndex();
	}

	public void updateQueue() {
		if (!updatePeriodically) {
			return;
		}
		synchronized (this) {
			for (int i = 0; i < backlogs.length; i++) {
				priorityQueue.changeKey(i, backlogs[i]);
			}
		}
	}

	/**
	 * @param obj
	 */
	@Override
	public void update(Object obj) {
		if (!(obj instanceof BacklogEvent)) {
			LOG.error("Error obj of update MinBacklogLoadBasedStrategy. Obj: {}", obj);
			throw new IllegalStateException("Error obj of update MinBacklogLoadBasedStrategy. Obj: " + obj);
		}
		BacklogEvent backlogEvent = (BacklogEvent) obj;
		int subPartitionIndex = backlogEvent.getSubPartitionIndex();
		int backlog = backlogEvent.getBacklog();
		if (updatePeriodically) {
			backlogs[subPartitionIndex] = backlog;
		} else {
			synchronized (this) {
				priorityQueue.changeKey(subPartitionIndex, backlog);
			}
		}
	}

	@Override
	public String toString() {
		return "MinBacklogLoadBasedStrategy{" +
			"updateInterval=" + updateInterval +
			'}';
	}
}
