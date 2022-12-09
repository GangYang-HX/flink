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

package org.apache.flink.runtime.speculativeframe;

public class Constants {

    public static final int DEFAULT_MIN_NUM_DATA_POINTS = 100;

    public static final SpeculativeScope DEFAULT_SPECULATIVE_SCOPE = SpeculativeScope.JobManager;

    public static final SpeculativeThresholdType DEFAULT_THRESHOLD_TYPE = SpeculativeThresholdType.NINETY_NINE;

    public static final long DEFAULT_MIN_THRESHOLDS = 0L;

    public static final long DEFAULT_MIN_GAP_THRESHOLDS = 0L;

    public static final int HISTOGRAM_WINDOW_SIZE = 1_000;

    public static final long DEFAULT_NOTIFY_INTERVAL_MS = 10_000;



    //-------------------------------metrics---------------------------//

    public static final String SPECULATIVE_METRIC_GROUP = "speculative";

    public static final String SPECULATIVE_TASK_TYPE = "type";

    public static final String TOTAL_NUM_OF_TASKS = "totalNumOfTasks";

    public static final String NUM_OF_TASKS_SPECULATED = "numOfTasksSpeculated";

    public static final String SPECULATIVE_THRESHOLD_GAUGE = "speculatedValues";
}
