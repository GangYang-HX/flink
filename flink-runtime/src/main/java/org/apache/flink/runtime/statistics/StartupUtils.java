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

import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import org.apache.commons.collections.CollectionUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Utility to startup cluster and job.
 */
public class StartupUtils {

	private static final Map<String, Map<StartupPhase, Object>> startTimestamp = new ConcurrentHashMap<>();

	/**
	 * 	When the phase which have multiple subPhases, there should be handled specially.
	 * 	If the phase has multi subPhases, every subPhases should be recorded.
	 * 	There are special handling in {@link StartupUtils#setTimestampByPhase} and
	 * 	{@link StartupUtils#getTimestampByPhase}
 	 */
	private static final Set<StartupPhase> multiSubPhases = ImmutableSet.of(
			StartupPhase.YARN_ALLOCATE_CONTAINER,
			StartupPhase.DEPLOY_TASK);

	/**
	 * If a job has been restarted, The job should be added to the map.
	 * It records the number of times the job was started.
	 */
	private static final Map<String, Integer> restartedJobs = new ConcurrentHashMap<>();

	// These variables will record the start time of each stage in the form of environment variables.
	public static final String ENV_SUBMIT_JOB_MANAGER = "SUBMIT_JOB_MANAGER";

	public static boolean isRestartedJob(String jobID){
		return restartedJobs.containsKey(jobID);
	}

	public static void incrementRestartedCountByJobID(String jobID) {
		if (!StringUtils.isNullOrWhitespaceOnly(jobID)) {
			int restartCount = restartedJobs.getOrDefault(jobID, 0);
			restartedJobs.put(jobID, restartCount + 1);
		}
	}

	public static int getRestartedCountByJobID(String jobID) {
		return restartedJobs.getOrDefault(jobID, 0);
	}

	public static void setTimestampByPhase(String traceID, StartupPhase phase, long timeStamp) {
		if (!multiSubPhases.contains(phase)) {
			startTimestamp.computeIfAbsent(traceID, key -> new ConcurrentHashMap<>()).put(phase, timeStamp);
		} else {
			((CopyOnWriteArrayList<Long>) startTimestamp.computeIfAbsent(traceID, traceKey -> new ConcurrentHashMap<>())
					.computeIfAbsent(phase, phaseKey -> new CopyOnWriteArrayList<>())).add(timeStamp);
		}
	}

	public static Object getTimestampByPhase(String traceID, StartupPhase phase) {
		if (!multiSubPhases.contains(phase)) {
			if (startTimestamp.containsKey(traceID) && startTimestamp.get(traceID).containsKey(phase)) {
				return startTimestamp.get(traceID).get(phase);
			}
		} else {
			Object o;
			if (startTimestamp.get(traceID) != null && (o = startTimestamp.get(traceID).get(phase)) != null) {
				CopyOnWriteArrayList<Long> timestamps = (CopyOnWriteArrayList<Long>) o;
				if (CollectionUtils.isNotEmpty(timestamps)) {
					return timestamps.get(0);
				}
			}
		}
		return null;
	}

	public static void removeTimestampByPhase(String traceID, StartupPhase phase) {
		if (!multiSubPhases.contains(phase)) {
			if (startTimestamp.containsKey(traceID)) {
				startTimestamp.get(traceID).remove(phase);
			}
		} else {
			Object o;
			if (startTimestamp.get(traceID) != null && (o = startTimestamp.get(traceID).get(phase)) != null) {
				CopyOnWriteArrayList<Long> timestamps = (CopyOnWriteArrayList<Long>) o;
				if (CollectionUtils.isNotEmpty(timestamps)) {
					timestamps.remove(0);
				}
			}
		}
	}


	/**
	 * An enumeration of important and time-cost phases during starting up a Flink Job.
	 *
	 * <p>The order of phases is as follows:
	 *
	 * <pre>{@code
	 *
	 * |--------------------------------------------------------------Client
	 * |---------------------------CLIENT_START_TIMESTAMP
	 * |---------------------------UPLOAD_FILES_TO_FILESYSTEM
	 * |---------------------------SUBMIT_JOB_MANAGER
	 * |---------------RUN_CLUSTER_ENTRYPOINT(run in JobManager)
	 * |---------------------------SUBMIT_JOB_TO_DISPATCHER
	 * |---------------------------CLIENT_START_JOB_REQUEST
	 * |--------------------------------------------------------------Client
	 * |
	 * |--------------------------------------------------------------JobManager
	 * |------------------------------------------RUN_CLUSTER_ENTRYPOINT
	 * |------------------------------------------[START|RESTART]_JOB
	 * |---------------------------CREATE_EXECUTION_AT_[START|RESTART]
	 * |---------------------------ALLOCATE_ALL_SLOTS_AT_[START|RESTART]
	 * |---------------ALLOCATE_SLOT
	 * |---------------REQUEST_All_SLOTS_FROM_RESOURCE_MANAGER
	 * |-------REQUEST_SLOT_FROM_RESOURCE_MANAGER
	 * |---------------YARN_ALLOCATE_ALL_CONTAINERS
	 * |-------YARN_ALLOCATE_CONTAINER(run in YARN)
	 * |---------------YARN_INITIAL_AND_START_ALL_CONTAINERS
	 * |-------START_TASK_EXECUTOR(run in TaskManager)
	 * |-------OFFER_SLOT_TO_JOB_MASTER(run in TaskManager)
	 * |---------------------------DEPLOY_ALL_TASKS_AT_[START|RESTART]
	 * |---------------DEPLOY_TASK
	 * |--------------------------------------------------------------JobManager
	 *
	 * }</pre>
	 *
	 */
	public enum StartupPhase {

		//----------------------------------------------------------------------------------------------
		// client
		//----------------------------------------------------------------------------------------------

		/**
		 * Client starts session cluster.
		 */
		CLIENT_START_SESSION,

		/**
		 * Client upload jar, config and other files to FileSystem needed for a Job.
		 */
		CLIENT_START_JOB_REQUEST,

		/**
		 * The total time of flink client.
		 */
		CLIENT_START_JOB,

		/**
		 * Client upload jar, config and other files to FileSystem needed for a Job.
		 */
		UPLOAD_FILES_TO_FILESYSTEM,

		/**
		 * Ask YARN to start AppMaster, and then deploy JobManager until finished.
		 */
		PREPARE_RESOURCE_FOR_JOB_MANAGER,

		/**
		 * Whole yarn starter phase, which is called by method of invokeInteractiveModeForExecution.
		 */
		INVOKE_USER_PROGRAM,

		/**
		 * Translate the buffered operations to Transformations.
		 */
		TRANSLATE_TO_TRANSFORMATION,

		/**
		 * Client generate JobGraph from user jar and submit it to dispatcher.
		 */
		SUBMIT_JOB_TO_DISPATCHER,

		//----------------------------------------------------------------------------------------------
		// JobManager
		//----------------------------------------------------------------------------------------------

		/**
		 * RocksDBStateBackend selects the highest score disk.
		 */
		SELECT_DISK_BY_ROCKSDB,

		/**
		 * JobManager receive request from client and jobManager container starts.
		 */
		SUBMIT_JOB_MANAGER,

		/**
		 * Run ClusterEntrypoint, which is Flink's enter class, including starting Dispatcher
		 * and ResourceManager.
		 *
		 * <p>Note: this phase is included in above phase {@code SUBMIT_JOB_MANAGER}.
		 */
		START_JOB_MANAGER,

		/**
		 * The phase from job manger start to reconcile completed.
		 */
		START_HA_RECONCILE,

		/**
		 * The whole time from first submitting  a job till successfully running a job.
		 */
		START_JOB,

		/**
		 * The whole time from client first submitting a job till successfully running a job.
		 */
		START_JOB_FROM_CLIENT,

		/**
		 * Runs the user program entrypoint and completes the given
		 */
		EXECUTE_PROGRAM,

		/**
		 * The whole time from recovering a job till successfully running a job.
		 */
		RESTART_JOB,

		/**
		 * Create Execution vertices at first start.
		 */
		CREATE_EXECUTION_AT_START,

		/**
		 * Create Execution vertices at restart.
		 */
		CREATE_EXECUTION_AT_RESTART,

		/**
		 * JobMaster ask ResourceManager to allocate one slot, if ResourceManager doesn't have
		 * free slots, it will ask YARN to allocate container and start Flink's TaskManager
		 * on the container.
		 */
		ALLOCATE_SLOT,

		/**
		 * JobMaster ask ResourceManager to allocate all slots needed for a Job at first start.
		 */
		ALLOCATE_ALL_SLOTS_AT_START,

		/**
		 * JobMaster ask ResourceManager to allocate all slots needed for a Job at restart.
		 */
		ALLOCATE_ALL_SLOTS_AT_RESTART,

		/**
		 * JobMaster starts a slot request to ResourceManager, and it allocates a slot if had.
		 *
		 * <p>Note: this phase doesn't contain the time ResourceManager ask resource from YARN/K8S.
		 */
		REQUEST_SLOT_FROM_RESOURCE_MANAGER,

		/**
		 * JobMaster request all slots from ResourceManager.
		 *
		 * <p>Note: this phase doesn't contain the time ResourceManager ask resource from YARN/K8S.
		 */
		REQUEST_All_SLOTS_FROM_RESOURCE_MANAGER,

		/**
		 * YARN's RM allocates a container to YarnResourceManager.
		 */
		YARN_ALLOCATE_CONTAINER,

		/**
		 * YARN's RM allocates all container to YarnResourceManager.
		 */
		YARN_ALLOCATE_ALL_CONTAINERS,

		/**
		 * YARN initialize, create and launch all allocated containers.
		 */
		YARN_INITIAL_AND_START_ALL_CONTAINERS,

		/**
		 * JobMaster deploy and run a task to TaskManager's slot after slot is allocated,
		 * this phase will finish until the task's status changes to RUNNING.
		 */
		DEPLOY_TASK,

		/**
		 * JobMaster deploy and run all tasks to TaskManager at first start.
		 */
		DEPLOY_ALL_TASKS_AT_START,

		/**
		 * JobMaster reconciling start and all task status is RUNNING switched from FAILED.
		 */
		RECONCILING_AT_START,

		/**
		 * JobMaster deploy and run all tasks to TaskManager at restart.
		 */
		DEPLOY_ALL_TASKS_AT_RESTART,

		//----------------------------------------------------------------------------------------------
		// TaskManager
		//----------------------------------------------------------------------------------------------

		START_CONTAINER,
		/**
		 * After YARN allocate container, start YarnTaskExecutorRunner,
		 * which is TaskManager's enter class.
		 *
		 * <p>Note: this phase is included in above phase {@code ALLOCATE_SLOT}.
		 */
		START_TASK_EXECUTOR,

		/**
		 * TaskExecutor offer allocated slot to JobMaster.
		 */
		OFFER_SLOT_TO_JOB_MASTER,

		/**
		 * Task load jar files in TaskManager
		 */
		TASK_LOAD_JAR_FILES,

		TASK_EXECUTOR_SUBMIT_TASK
	}

}
