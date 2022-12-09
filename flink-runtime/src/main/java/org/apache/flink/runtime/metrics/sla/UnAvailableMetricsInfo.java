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

import java.util.HashMap;
import java.util.Map;

/**
 * UnAvailableMetricsInfo pojo. It conatins some necessary information.
 */
public class UnAvailableMetricsInfo {

    private final String jobId;

    private final long unAvailableMetricValue;

    private final long lastEndTimestamp;

    private final long currentStartTimestamp;

    private final CauseType causeType;

    private final long createTime;

    public UnAvailableMetricsInfo(
            String jobId,
            long unAvailableMetricValue,
            long lastEndTimestamp,
            long currentStartTimestamp,
            CauseType causeType,
            long createTime) {
        this.jobId = jobId;
        this.unAvailableMetricValue = unAvailableMetricValue;
        this.lastEndTimestamp = lastEndTimestamp;
        this.currentStartTimestamp = currentStartTimestamp;
        this.causeType = causeType;
        this.createTime = createTime;
    }

    public Map<String, String> generateDimensions() {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("jobId", String.valueOf(jobId));
        dimensions.put("unAvailableMetricValue", String.valueOf(unAvailableMetricValue));
        dimensions.put("lastEndTimestamp", String.valueOf(lastEndTimestamp));
        dimensions.put("currentStartTimestamp", String.valueOf(currentStartTimestamp));
        dimensions.put("causeType", String.valueOf(causeType));
        dimensions.put("createTime", String.valueOf(createTime));
        return dimensions;
    }

    @Override
    public String toString() {
        return "UnAvailableMetricsInfo{" +
                "jobId='" + jobId + '\'' +
                ", unAvailableMetricValue=" + unAvailableMetricValue +
                ", lastEndTimestamp=" + lastEndTimestamp +
                ", currentStartTimestamp=" + currentStartTimestamp +
                ", causeType=" + causeType +
                ", createTime=" + createTime +
                '}';
    }
}
