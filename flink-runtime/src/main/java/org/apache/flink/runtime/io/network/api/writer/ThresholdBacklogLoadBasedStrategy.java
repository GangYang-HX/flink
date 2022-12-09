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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * This is a load based implementation of the {@link LoadBasedStrategy} interface based on threshold backlog. Generally,
 * it distributes the data to the channel which it's backlog value below the threshold.
 */
public final class ThresholdBacklogLoadBasedStrategy implements LoadBasedStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(ThresholdBacklogLoadBasedStrategy.class);

	private static final double DEFAULT_THRESHOLD_FACTOR = 1.3;

	private static final int DEFAULT_FREQUENCY_COUNT = 50;

	private int numberOfChannels;

	/**
	 * Stores the index of the channel to send the next record to.
	 */
	private int nextChannel = -1;

	private double threshold;

	private int[] backlogs;

	private double increment;

	private final double thresholdFactor;

	private final int updateFrequencyCount;

	private long backlogUpdateTimes = 0;

	public ThresholdBacklogLoadBasedStrategy() {
		this(DEFAULT_THRESHOLD_FACTOR, DEFAULT_FREQUENCY_COUNT);
	}

	public ThresholdBacklogLoadBasedStrategy(double thresholdFactor, int updateFrequencyCount) {
		this.thresholdFactor = thresholdFactor;
		this.updateFrequencyCount = updateFrequencyCount;
	}

	@Override
	public void setup(int numberOfChannels) {
		this.numberOfChannels = numberOfChannels;
		backlogs = new int[numberOfChannels];
		increment = thresholdFactor / numberOfChannels;
	}

	@Override
	public int select(final IOReadableWritable record) {
		nextChannel = nextChannel(nextChannel, numberOfChannels);
		int i = 0;
		// Find the backlog of next channel that is less than or equal to the threshold value.
		while (backlogs[nextChannel] > threshold && i++ < numberOfChannels) {
			nextChannel = nextChannel(nextChannel, numberOfChannels);
		}
		return nextChannel;
	}

	/**
	 * @param obj
	 */
	public void update(Object obj) {
		if (!(obj instanceof BacklogEvent)) {
			LOG.error("Error obj of update ThresholdBacklogLoadBasedStrategy. Obj: {}", obj);
			throw new IllegalStateException("Error obj of update ThresholdBacklogLoadBasedStrategy. Obj: " + obj);
		}
		BacklogEvent backlogEvent = (BacklogEvent) obj;
		int subPartitionIndex = backlogEvent.getSubPartitionIndex();
		int backlog = backlogEvent.getBacklog();
		int oldValue = backlogs[subPartitionIndex];
		backlogs[subPartitionIndex] = backlog;

		// The threshold value is inaccurate because of multithreading concurrency and accuracy loss.
		if (oldValue > backlog) {
			threshold -= increment;
		} else {
			threshold += increment;
		}

		// Periodic calibration threshold value.
		if (++backlogUpdateTimes % updateFrequencyCount == 0) {
			double newThreshold = Arrays.stream(backlogs).sum() * thresholdFactor / numberOfChannels;
			LOG.debug("Threshold: {}, newThreshold: {}, backlogs: {}", threshold, newThreshold, backlogs);
			threshold = newThreshold;
		}
	}

	@Override
	public String toString() {
		return "ThresholdBacklogLoadBasedStrategy{" +
			"thresholdFactor=" + thresholdFactor +
			", updateFrequencyCount=" + updateFrequencyCount +
			'}';
	}
}
