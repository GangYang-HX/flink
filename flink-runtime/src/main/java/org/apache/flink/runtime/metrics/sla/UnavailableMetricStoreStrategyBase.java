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

public abstract class UnavailableMetricStoreStrategyBase implements UnavailableMetricStoreStrategy {
    protected final JobID jobID;

    public UnavailableMetricStoreStrategyBase(JobID jobID) {
        this.jobID = jobID;
    }

    @Override
    public void terminateHook(CauseType type, long timestamp, boolean restarted) {
        UnAvailableMetricsInfo metricsInfo = getTerminateMetricInfo(type, timestamp);
        UnavailableMetricStoreStrategy.sendMetricInfo(metricsInfo);
    }

    @Override
    public void startUpHook(long timestamp) {
        UnAvailableMetricsInfo metricsInfo = getStartUpMetricInfo(timestamp);
        UnavailableMetricStoreStrategy.sendMetricInfo(metricsInfo);
    }

    public UnAvailableMetricsInfo getStartUpMetricInfo(long timestamp) {
        return new UnAvailableMetricsInfoBuilder()
                .setCurrentStartTimestamp(timestamp)
                .setJobId(jobID.toHexString())
                .build();
    }

    public UnAvailableMetricsInfo getTerminateMetricInfo(CauseType type, long timestamp) {
        return new UnAvailableMetricsInfoBuilder()
                .setLastEndTimestamp(timestamp)
                .setCauseType(type)
                .setJobId(jobID.toHexString())
                .build();
    }
}
