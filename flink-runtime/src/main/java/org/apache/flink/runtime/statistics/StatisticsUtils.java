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

package org.apache.flink.runtime.statistics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.RestoreCheckpointException;
import org.apache.flink.runtime.checkpoint.RestoreFailureReason;
import org.apache.flink.runtime.statistics.metrics.Slf4jOnceSender;
import org.apache.flink.util.ExceptionUtils;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatisticsUtils {
	public static final String STARTUP_TIME = "startupTime";

	public static final String PHASE = "period";

	public static final String JOB_ID = "job_id";

	public static final String JOB_INFO = "jobInfo";

	public static final String CHECKPOINT_RESTORE_INFO = "checkpoint_restore_info";

	private static final String TIMESTAMP = "timestamp";

	public static final String JOB_STATUS = "job_status";

	public static final String STATUS = "status";

	public static final String STARTED_STATUS = "started";

	public static final String SUCCESS_STATUS = "success";

	public static final String FAILURE_STATUS = "failure";

	public static final String CAUSE = "cause";

	public static final String ROOT_CAUSE_TYPE = "root_cause_type";

	private static final String RESTORE_PATH = "restore_path";

	public static final String START_TIME = "start_time";

	public static final String TASK_NAME = "task_name";

	public static final String TASK_INDEX = "task_index";

	public static final String CONTAINER_ID = "container_id";

	public static final String NODE_ID = "tm_id";

	public static final String HOST = "host";

	public static final String RESTART_COUNT = "restart_count";

	public static final String YARN = "yarn";

	/* The default truncated message length. */
	private static final int DEFAULT_CAUSE_MESSAGE_LENGTH = 100;

	public static final List<Sender> senders = new ArrayList<>();

	/**
	 * Report flink statistics data. This method can be called by other components
	 * to send data for once.
	 *
	 * @param name statistics data's name
	 * @param value statistics data's value
	 * @param dimensions K-V pairs which represent features of the statistics data
	 */
	public static void report(
			String name,
			String value,
			Map<String, String> dimensions) {
		Slf4jOnceSender.getInstance().send(name, value, dimensions);
	}

	public static void reportStartTracking(StartupUtils.StartupPhase phase){
		reportStartTracking(phase, System.currentTimeMillis());
	}

	public static void reportStartTracking(StartupUtils.StartupPhase phase, long timestamp){
		senders.forEach(sender -> {
			sender.sendStartTracking(phase, timestamp);
		});
	}

	public static void reportStartTracking(StartupUtils.StartupPhase phase, String timestamp){
		senders.forEach(sender -> {
			sender.sendStartTracking(phase, Long.parseLong(timestamp));
		});
	}

	public static void reportEndTracking(StartupUtils.StartupPhase phase) {
		reportEndTracking(phase, true, null, null);
	}

	public static void reportEndTracking(StartupUtils.StartupPhase phase, boolean isSucceeded) {
		reportEndTracking(phase, isSucceeded, null, null);
	}

	public static void reportEndTracking(
			StartupUtils.StartupPhase phase,
			boolean isSucceeded,
			Map<String, String> dimensions) {
		reportEndTracking(phase, isSucceeded, null, dimensions);
	}

	public static void reportEndTracking(
			StartupUtils.StartupPhase phase,
			boolean isSucceeded,
			String jobID,
			Map<String, String> dimensions) {
		Map<String, String> metadata = null;
		if (MapUtils.isNotEmpty(dimensions)) {
			metadata = new HashMap<>();
			metadata.putAll(dimensions);
		}
		if (StringUtils.isNoneEmpty(jobID)) {
			if (metadata == null) {
				metadata = new HashMap<>();
			}
			metadata.put(JOB_ID, jobID);
		}
		for (Sender sender : senders) {
			sender.sendEndTracking(phase, isSucceeded, metadata);
		}
	}

	/**
	 * Static method to report job info
	 *
	 * @param jobId Flink job id
	 * @param jobStatus Flink job status
	 * @param timestamp report timestamp
	 * @param status the job report status, include started, success, and failure
	 * @param globalModVersion the ExecutionGraph globalModVersion
	 * @param cause the cause when the job is failure
	 */
	public static void reportJobInfo(
			String jobId,
			JobStatus jobStatus,
			long timestamp,
			String status,
			long globalModVersion,
			Throwable cause) {
		Map<String, String> metadata = new HashMap<>();
		metadata.put(JOB_ID, jobId);
		metadata.put(TIMESTAMP, String.valueOf(timestamp));
		metadata.put(JOB_STATUS, jobStatus.toString());
		metadata.put(STATUS, status);
		if (cause != null) {
			metadata.put(CAUSE, ExceptionUtils.getTruncateRootCauseMessage(cause, DEFAULT_CAUSE_MESSAGE_LENGTH));
		}
		report(JOB_INFO, String.valueOf(globalModVersion), metadata);
	}

	public static void reportCheckpointRestoreInfo(
			String jobId,
			String restorePath,
			long timestamp,
			String status,
			Throwable cause) {
		Map<String, String> metadata = new HashMap<>();
		metadata.put(JOB_ID, jobId);
		metadata.put(TIMESTAMP, String.valueOf(timestamp));
		metadata.put(STATUS, status);
		metadata.put(RESTORE_PATH, restorePath);
		if (cause != null) {
			if (cause instanceof RestoreCheckpointException) {
				RestoreCheckpointException restoreCheckpointException = (RestoreCheckpointException) cause;
				metadata.put(ROOT_CAUSE_TYPE, restoreCheckpointException.getRestoreFailureReason().getMessage());
			} else {
				metadata.put(ROOT_CAUSE_TYPE, RestoreFailureReason.UNKNOWN_EXCEPTION.getMessage());
			}
			metadata.put(CAUSE, ExceptionUtils.getTruncateRootCauseMessage(cause, DEFAULT_CAUSE_MESSAGE_LENGTH));
		}
		report(CHECKPOINT_RESTORE_INFO, status, metadata);
	}
}
