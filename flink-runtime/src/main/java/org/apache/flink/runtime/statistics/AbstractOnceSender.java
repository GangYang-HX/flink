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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.statistics.datatrace.TraceOnceSender;
import org.apache.flink.runtime.statistics.metrics.Slf4jOnceSender;
import org.apache.flink.runtime.util.MapUtil;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class AbstractOnceSender implements Sender {

	public static final String JSON_NAME = "name";
	public static final String JSON_VALUE = "value";
	public static final String JSON_LABELS = "labels";
	public static final String JSON_GAUGE = "gauge";

	public static final String DIMENSION_YARN_APPLICATION_ID = "yarn_application_id";
	public static final String DIMENSION_YARN_QUEUE_NAME = "yarn_queue_name";
	public static final String DIMENSION_CLUSTER_TYPE = "cluster_type";
	public static final String DIMENSION_JOB_NAME = "job_name";
	public static final String DIMENSION_FLINK_VERSION = "flink_version";

	public static final String SABER_METRICS_LABELS = "metrics.reporter.prom.labels";
	public static final String SABER_JOB_NAME = "saber_job_name";
	public static final String SABER_JOB_ID = "saber_job_id";

	public static final String CONTAINER_ID = "container_id";

	public static final String NODE_ID = "tm_id";

	public static final String HOST = "host";

	public static final String TRACE_ID = "trace_id";

	public static final String START_TIME = "start_time";

	public static final String END_TIME = "end_time";

	public static final String QUEUE_NAME = "queue_name";
	public static final String YARN_APPLICATION_QUEUE = "yarn.application.queue";
	public static final String FLINK_VERSION = "flink.version";
	public static final String FLINK_INNER_VERSION = "flink.inner.version";

	//===========================Trace configuration====================================

	public static final String DATATRACE_CLIENT_APP = "datatrace.client.app";

	public static final String DATATRACE_CLIENT_SOURCE = "datatrace.client.source";

	public static final String DATATRACE_CLIENT_REPORT_MODE = "datatrace.client.report_mode";

	private static final int NUMBER_OF_FIX_THREADS = 2;

	public static final Executor logExecutor = Executors.newFixedThreadPool(NUMBER_OF_FIX_THREADS);
	protected static Map<String, String> systemDimensions = new HashMap<>();

	/**
	 * In order to prevent the Sender implementation class from parsing the config,
	 * initOnceSender must be executed after parseSystemDimensions. Otherwise,
	 * some dimensions will be missing.
	 */
	public static void initialize(Configuration config) {
		parseSystemDimensions(config);
		if (StatisticsUtils.senders.isEmpty()){
			initLoadOnceSenders();
		}
	}

	public static void parseSystemDimensions(Configuration config){
		// default get from parameters set by saber:
		// -Dmetrics.reporter.slf4j.labels=saber_job_name=%s;job_owner=%s;queue_name=%s
		Map<String, String> dimensions = MapUtil.parseKvString(
				config.getString(SABER_METRICS_LABELS, ""));

		// If traceID is null, reset traceID to applicationID.
		if (!dimensions.containsKey(TRACE_ID)){
			dimensions.put(TRACE_ID, config.get(HighAvailabilityOptions.HA_CLUSTER_ID));
		}

		if (dimensions.containsKey(SABER_JOB_NAME)) {
			dimensions.put(DIMENSION_JOB_NAME, dimensions.get(SABER_JOB_NAME));
		}

		String queue = dimensions.get(QUEUE_NAME);
		if (StringUtils.isNullOrWhitespaceOnly(queue)) {
			queue = config.getString(YARN_APPLICATION_QUEUE, "");
		}
		dimensions.put(DIMENSION_YARN_QUEUE_NAME, queue);

		String clusterType = dimensions.get(DIMENSION_CLUSTER_TYPE);
		if (StringUtils.isNullOrWhitespaceOnly(clusterType)) {
			clusterType = config.getString(DeploymentOptions.CLUSTER_TYPE);
		}
		dimensions.put(DIMENSION_CLUSTER_TYPE, clusterType);

		dimensions.put(DIMENSION_YARN_APPLICATION_ID,
				config.get(HighAvailabilityOptions.HA_CLUSTER_ID));

		String flinkVersion = config.getString(FLINK_VERSION, "");
		if (StringUtils.isNullOrWhitespaceOnly(flinkVersion)) {
			flinkVersion = config.getString(FLINK_INNER_VERSION, "");
		}
		dimensions.put(DIMENSION_FLINK_VERSION, flinkVersion);

		config.getOptional(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID)
				.ifPresent(saberJobId -> dimensions.put(SABER_JOB_ID, saberJobId));

		dimensions.put(
			CONTAINER_ID,
			System.getenv(ApplicationConstants.Environment.CONTAINER_ID.key()));
		dimensions.put(HOST, System.getenv(ApplicationConstants.Environment.NM_HOST.key()));

		systemDimensions = Collections.unmodifiableMap(dimensions);
	}

	public static void initLoadOnceSenders(){
		StatisticsUtils.senders.add(new Slf4jOnceSender());
		StatisticsUtils.senders.add(new TraceOnceSender());
	}

	@Override
	public void send(String name, String value, Map<String, String> dimensions) {

	}
}
