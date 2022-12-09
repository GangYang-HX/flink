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

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Used to request to jobmanager within a interval time and each corresponding to a task.
 */
public class SubTaskStatus implements Serializable {

	private static final long serialVersionUID = 2810352616205751385L;

	private final int subTaskId;

	private long watermark;

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private long currTime;

	TaskStatusType taskStatusType;

	public enum TaskStatusType {
		/**
		 * means we update task status every 50 millisecond.
		 */
		UPDATE_TASK_STATUS,

		/**
		 * means we need rate limiter for tasks.
		 */
		CONTROL_SPEED
	}

	public SubTaskStatus(int subTaskId, long watermark, long currTime, TaskStatusType taskStatusType) {
		this.subTaskId = subTaskId;
		this.watermark = watermark;
		this.currTime = currTime;
		this.taskStatusType = taskStatusType;
	}

	public long getCtime() {
		return currTime;
	}

	public SubTaskStatus setCtime(long ctime) {
		this.currTime = ctime;
		return this;
	}

	public int getSubTaskId() {
		return subTaskId;
	}

	public long getWatermark() {
		return watermark;
	}

	public SubTaskStatus setWatermark(long watermark) {
		this.watermark = watermark;
		return this;
	}

	@Override
	public String toString() {
		try {
			return "SubTaskStatus{" +
				"subTaskId=" + subTaskId +
				", watermark=" + watermark +
				", also=" + sdf.parse(sdf.format(watermark)) +
				", currTime=" + sdf.parse(sdf.format(currTime)) +
				", type=" + taskStatusType +
				'}';
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "null";
	}
}
