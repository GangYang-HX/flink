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
 * Utility to job info.
 */
public class JobInfoUtils {
	private static final Logger log = LoggerFactory.getLogger(JobInfoUtils.class);

	private static final Map<JobID, Long> registerJobInfos = new ConcurrentHashMap<>();

	public static boolean registerStartedJob(JobID jobId, long globalModVersion){
		if (registerJobInfos.containsKey(jobId)) {
			log.warn("Ignore duplicated job registration for {}", jobId);
			return false;
		} else {
			registerJobInfos.put(jobId, globalModVersion);
			return true;
		}
	}

	public static Long deregisterStartedJob(JobID jobId){
		return registerJobInfos.remove(jobId);
	}

	public static Map<JobID, Long> getRegisterJobInfos() {
		return registerJobInfos;
	}

}
