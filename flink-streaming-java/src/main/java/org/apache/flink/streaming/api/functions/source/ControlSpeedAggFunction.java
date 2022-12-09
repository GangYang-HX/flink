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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gather the latest all task status for rate limiter every time.
 */
public class ControlSpeedAggFunction implements AggregateFunction<byte[], Map<Integer, SubTaskStatus>, byte[]> {

	private static final Logger LOG = LoggerFactory.getLogger(ControlSpeedAggFunction.class);

	private static final long serialVersionUID = 5093820210959172422L;

	private int numberOfParallelSubtasks;

	/**
	 * maximum time tolerance of watermark with different tasks.
	 */
	private int tolerateInterval;

	private int estimateTime;

	private static final short CONSTANT = 1_000;

	private static final short WARM_UP = 10_000;

	/**
	 * If set to false, we can see different source partition watermark gap without rate limiter enabled.
	 * Just for compare with the job turn on the rate limiter.
	 */
	private boolean isDebugControlSpeed;

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private final long firstTime = System.currentTimeMillis();

	private final ControlSpeed noControlSpeed = new ControlSpeed(Integer.MIN_VALUE, -1L);

	@Override
	public Map<Integer, SubTaskStatus> createAccumulator() {
		return new ConcurrentHashMap<>(numberOfParallelSubtasks);
	}

	@Override
	public Map<Integer, SubTaskStatus> add(byte[] value, Map<Integer, SubTaskStatus> accumulator) {
		SubTaskStatus subTaskStatus = null;
		try {
			subTaskStatus = InstantiationUtil.deserializeObject(value, this.getClass().getClassLoader());
			accumulator.put(subTaskStatus.getSubTaskId(), subTaskStatus);
			return accumulator;
		} catch (Exception e) {
			LOG.warn("Exception while deserialize subtask status {}.", subTaskStatus, e);
		}
		return Collections.emptyMap();
	}

	@Override
	public byte[] getResult(Map<Integer, SubTaskStatus> accumulator) {
		SubTaskStatus subTaskStatus = accumulator.get(1);
		if (System.currentTimeMillis() - firstTime > WARM_UP) {
			Tuple2<Long, Long> watermarks = maxMinWatermark(accumulator);
			try {
				if (overTolerateInterval(watermarks.f0, watermarks.f1)) {
					long targetWatermark = watermarks.f0;
					if (subTaskStatus.taskStatusType.equals(SubTaskStatus.TaskStatusType.CONTROL_SPEED)) {
						LOG.info(
							"max wm: " + sdf.parse(sdf.format(watermarks.f0)) + ", (" + maxWatermarkTaskId(accumulator) + "/" + numberOfParallelSubtasks + ")" + ", also: " + watermarks.f0 +
								", min wm: " + sdf.parse(sdf.format(watermarks.f1)) + ", (" + minWatermarkTaskId(accumulator) + "/" + numberOfParallelSubtasks + ")" + ", also: " + watermarks.f1 +
								", gap is " + (watermarks.f0 - watermarks.f1) / CONSTANT + "second, tarwm: " + targetWatermark + ", also: " + sdf.parse(sdf.format(targetWatermark)) +
								", ctime: " + sdf.parse(sdf.format(System.currentTimeMillis())) +
								", subTasks: " + accumulator.toString());
					}
					if (isDebugControlSpeed) {
						return InstantiationUtil.serializeObject(new ControlSpeed(estimateTime, targetWatermark));
					} else {
						return InstantiationUtil.serializeObject(noControlSpeed);
					}
				} else {
					// do not make control speed, we can not return null here in case of akka.time.out
					return InstantiationUtil.serializeObject(noControlSpeed);
				}
			} catch (Exception e) {
				LOG.warn("Exception while serialize control speed or failed to parse date.", e);
				return new byte[0];
			}
		} else {
			try {
				return InstantiationUtil.serializeObject(noControlSpeed);
			} catch (IOException e) {
				LOG.warn("Exception while serialize control speed.");
				return new byte[0];
			}
		}
	}

	private boolean overTolerateInterval(long maxWatermark, long minWatermark) {
		try {
			Date max = sdf.parse(sdf.format(maxWatermark));
			Date min = sdf.parse(sdf.format(minWatermark));
			return (max.getTime() - min.getTime()) > tolerateInterval * CONSTANT;
		} catch (Exception e) {
			LOG.warn("Failed to parse date.", e);
		}
		return false;
	}

	private Tuple2<Long, Long> maxMinWatermark(Map<Integer, SubTaskStatus> accumulator) {
		long maxWatermark = Long.MIN_VALUE;
		long minWatermark = Long.MAX_VALUE;
		Tuple2<Long, Long> maxMinWatermark = new Tuple2<>();
		for (SubTaskStatus subTaskStatus : accumulator.values()) {
			long currentTimeStamp = subTaskStatus.getWatermark();
			if (maxWatermark < currentTimeStamp) {
				maxWatermark = currentTimeStamp;
			}
			if (minWatermark > currentTimeStamp) {
				minWatermark = currentTimeStamp;
			}
		}
		maxMinWatermark.f0 = maxWatermark;
		maxMinWatermark.f1 = minWatermark;
		return maxMinWatermark;
	}

	// not use now
	public boolean taskStatusTimeGap(Map<Integer, SubTaskStatus> accumulator) {
		long maxTime = Long.MIN_VALUE;
		long minTime = Long.MAX_VALUE;

		for (SubTaskStatus subTaskStatus : accumulator.values()) {
			long currTime = subTaskStatus.getCtime();
			if (maxTime < currTime) {
				maxTime = currTime;
			}
			if (minTime > currTime) {
				minTime = currTime;
			}
		}
		return (maxTime - minTime) <= estimateTime;
	}

	@Override
	public Map<Integer, SubTaskStatus> merge(Map<Integer, SubTaskStatus> a, Map<Integer, SubTaskStatus> b) {
		return null;
	}

	@VisibleForTesting
	public int maxWatermarkTaskId(Map<Integer, SubTaskStatus> accumulator) {
		int taskId = Integer.MIN_VALUE;
		long maxWatermark = Long.MIN_VALUE;
		for (SubTaskStatus subTaskStatus : accumulator.values()) {
			if (subTaskStatus.getWatermark() > maxWatermark) {
				maxWatermark = subTaskStatus.getWatermark();
				taskId = subTaskStatus.getSubTaskId();
			}
		}
		return taskId + 1;
	}

	@VisibleForTesting
	public int minWatermarkTaskId(Map<Integer, SubTaskStatus> accumulator) {
		int taskId = Integer.MAX_VALUE;
		long minWatermark = Long.MAX_VALUE;
		for (SubTaskStatus subTaskStatus : accumulator.values()) {
			if (subTaskStatus.getWatermark() < minWatermark) {
				minWatermark = subTaskStatus.getWatermark();
				taskId = subTaskStatus.getSubTaskId();
			}
		}
		return taskId + 1;
	}

	public long getFirstTime() {
		return firstTime;
	}

	public boolean isDebugControlSpeed() {
		return isDebugControlSpeed;
	}

	public ControlSpeedAggFunction setDebugControlSpeed(boolean debugControlSpeed) {
		isDebugControlSpeed = debugControlSpeed;
		return this;
	}

	public int getNumberOfParallelSubtasks() {
		return numberOfParallelSubtasks;
	}

	public ControlSpeedAggFunction setNumberOfParallelSubtasks(int numberOfParallelSubtasks) {
		this.numberOfParallelSubtasks = numberOfParallelSubtasks;
		return this;
	}

	public int getTolerateInterval() {
		return tolerateInterval;
	}

	public ControlSpeedAggFunction setTolerateInterval(int tolerateInterval) {
		this.tolerateInterval = tolerateInterval;
		return this;
	}

	public int getEstimateTime() {
		return estimateTime;
	}

	public ControlSpeedAggFunction setEstimateTime(int estimateTime) {
		this.estimateTime = estimateTime;
		return this;
	}
}
