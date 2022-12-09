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

public class RestartAvailableStrategy extends UnavailableMetricStoreStrategyBase {

    private static final long serialVersionUID = 5327290722945270288L;

    private long lastTerminateTimeStamp;

    private CauseType lastCauseType = CauseType.FLINK_INTERNAL;

    private RestartAvailableStrategy(JobID jobID) {
        super(jobID);
    }


    @Override
    public UnAvailableMetricsInfo getStartUpMetricInfo(long timestamp) {
        UnAvailableMetricsInfoBuilder builder = new UnAvailableMetricsInfoBuilder();
        if (lastTerminateTimeStamp == 0) {
            // This indicates that the job is started for the first time.
            builder.setCurrentStartTimestamp(timestamp);
        } else {
            builder.setLastEndTimestamp(lastTerminateTimeStamp);
            builder.setCurrentStartTimestamp(timestamp);
            builder.setUnAvailableMetricValue(timestamp - lastTerminateTimeStamp);
            builder.setCauseType(lastCauseType);
        }
        builder.setJobId(jobID.toHexString());
        return builder.build();
    }

    @Override
    public void terminateHook(CauseType type, long timestamp, boolean restarted) {
        if (restarted) {
            // It doesn't restart anymore, we need to send a metric record.
            UnAvailableMetricsInfo metricsInfo = new UnAvailableMetricsInfoBuilder()
                    .setLastEndTimestamp(timestamp)
                    .setCauseType(type)
                    .setJobId(jobID.toHexString())
                    .build();
            UnavailableMetricStoreStrategy.sendMetricInfo(metricsInfo);
            return;
        }
        switch (type) {
            case CANCELLED:
            case HDFS:
            case MYSQL:
                UnAvailableMetricsInfo metricsInfo = new UnAvailableMetricsInfoBuilder()
                        .setLastEndTimestamp(timestamp)
                        .setCauseType(type)
                        .setJobId(jobID.toHexString())
                        .build();
                UnavailableMetricStoreStrategy.sendMetricInfo(metricsInfo);
                break;
            case FLINK_INTERNAL:
                lastTerminateTimeStamp = timestamp;
                lastCauseType = type;
                // We don't need to send metric record.
                break;
        }
    }

    /**
     * The factory to instantiate {@link RestartAvailableStrategy}.
     */
    public static class Factory implements UnavailableMetricStoreStrategy.Factory {

        @Override
        public UnavailableMetricStoreStrategy create(JobID jobID) {
            return new RestartAvailableStrategy(jobID);
        }
    }
}
