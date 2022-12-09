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

import org.apache.flink.util.Preconditions;

public class UnAvailableMetricsInfoBuilder {

    private String jobId;

    private long unAvailableMetricValue;

    private long lastEndTimestamp;

    private long currentStartTimestamp;

    private CauseType causeType = CauseType.NULL;

    public UnAvailableMetricsInfoBuilder setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public UnAvailableMetricsInfoBuilder setUnAvailableMetricValue(long unAvailableMetricValue) {
        this.unAvailableMetricValue = unAvailableMetricValue;
        return this;
    }

    public UnAvailableMetricsInfoBuilder setLastEndTimestamp(long lastEndTimestamp) {
        this.lastEndTimestamp = lastEndTimestamp;
        return this;
    }

    public UnAvailableMetricsInfoBuilder setCurrentStartTimestamp(long currentStartTimestamp) {
        this.currentStartTimestamp = currentStartTimestamp;
        return this;
    }

    public UnAvailableMetricsInfoBuilder setCauseType(CauseType causeType) {
        this.causeType = causeType;
        return this;
    }

    public UnAvailableMetricsInfo build() {
        // value check
        Preconditions.checkArgument(unAvailableMetricValue >=0, "unAvailableMetricValue");
        Preconditions.checkArgument(lastEndTimestamp >=0, "lastEndTimestamp");
        Preconditions.checkArgument(currentStartTimestamp >=0, "currentStartTimestamp");

        if (unAvailableMetricValue ==0 && lastEndTimestamp > 0 && currentStartTimestamp > 0) {
            throw new IllegalArgumentException(
                    "lastEndTimestamp and currentStartTimestamp cannot both be greater than 0");
        }
        return new UnAvailableMetricsInfo(
                jobId,
                unAvailableMetricValue,
                lastEndTimestamp,
                currentStartTimestamp,
                causeType,
                System.currentTimeMillis());
    }
}
