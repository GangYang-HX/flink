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

import org.apache.flink.api.common.JobID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility to restore from checkpoint info.
 */
public class CheckpointRestoreInfoUtils {
	private static final Logger log = LoggerFactory.getLogger(CheckpointRestoreInfoUtils.class);

	/* Map from job ID to the restore path. */
	private static final Map<JobID, String> registerCheckpointRestoreInfos = new ConcurrentHashMap<>();

	public static boolean registerCheckpointRestoreJob(JobID jobId, String restorePath){
		if (registerCheckpointRestoreInfos.containsKey(jobId)) {
			log.warn("Ignore duplicated job registration for {}", jobId);
			return false;
		} else {
			registerCheckpointRestoreInfos.put(jobId, restorePath);
			return true;
		}
	}

	public static String deregisterCheckpointRestoreJob(JobID jobId){
		return registerCheckpointRestoreInfos.remove(jobId);
	}

	public static String getRestorePath(JobID jobID) {
		return registerCheckpointRestoreInfos.get(jobID);
	}
}
