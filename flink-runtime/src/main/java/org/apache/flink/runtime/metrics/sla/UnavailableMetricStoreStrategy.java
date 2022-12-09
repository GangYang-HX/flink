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

package org.apache.flink.runtime.metrics.sla;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.statistics.metrics.Slf4jOnceSender;

import java.io.Serializable;

/**
 * Interface for recording a timestamp for un-available metric. It will fire
 * at job status transitions to ALL_TASKS_RUNNING, FAILED or CANCELD.
 */
public interface UnavailableMetricStoreStrategy extends Serializable {

    String METRIC_NAME = "unavailable_time";

    /**
     * Triggered when job status transitions to end.
     *
     * @param type      cause type
     * @param timestamp timestamp
     * @param restarted restarted
     */
    void terminateHook(CauseType type, long timestamp, boolean restarted);

    /**
     * Triggered when job status transitions to start.
     *
     * @param timestamp timestamp
     */
    void startUpHook(long timestamp);

    /**
     * It's used to send a metric info directly without creating specific strategy.
     * @param metricsInfo The info to send.
     */
    static void sendMetricInfo(UnAvailableMetricsInfo metricsInfo) {
        Slf4jOnceSender.getInstance().send(METRIC_NAME, "", metricsInfo.generateDimensions());
    }

    /**
     * The factory to instantiate {@link UnavailableMetricStoreStrategy}.
     */
    interface Factory {

        /**
         * Instantiates the {@link UnavailableMetricStoreStrategy}
         * @param jobID the JobID
         * @return The instantiated unavailable metrics store strategy.
         */
        UnavailableMetricStoreStrategy create(JobID jobID);

    }

}
