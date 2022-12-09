/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem.metrics;

public class HadoopFileMetricsConstants {

	public static final String HADOOP_FILE_METRICS_GROUP = "HadoopFile";

	public static final String OPEN_FILE_COST_TIME_METRICS = "openCostTime";

	public static final String FLUSH_FILE_COST_TIME_METRICS = "flushCostTime";

	public static final String CLOSE_FILE_COST_TIME_METRICS = "closeCostTime";

}
