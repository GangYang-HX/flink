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

package org.apache.flink.runtime.speculativeframe.rpc;

import org.apache.flink.runtime.speculativeframe.SpeculativeThresholdType;

import java.io.Serializable;

public class NeedReportToCoordinatorProperties implements Serializable {

    private Integer minDatapoints;

    private String thresholdType;

    private Long minGapOfThresholds;


    private NeedReportToCoordinatorProperties(
            Integer minDatapoints,
            SpeculativeThresholdType thresholdType,
            Long minGapOfThresholds) {
        this.minDatapoints = minDatapoints;
        this.thresholdType = thresholdType.name();
        this.minGapOfThresholds = minGapOfThresholds;
    }

    public Integer getMinDatapoints() {
        return minDatapoints;
    }

    public void setMinDatapoints(Integer minDatapoints) {
        this.minDatapoints = minDatapoints;
    }

    public SpeculativeThresholdType getThresholdType() {
        return SpeculativeThresholdType.valueOf(thresholdType);
    }

    public void setThresholdType(String thresholdType) {
        this.thresholdType = thresholdType;
    }

    public Long getMinGapOfThresholds() {
        return minGapOfThresholds;
    }

    public void setMinGapOfThresholds(Long minGapOfThresholds) {
        this.minGapOfThresholds = minGapOfThresholds;
    }

    public static NeedReportToCoordinatorProperties of(
            Integer minDatapoints,
            SpeculativeThresholdType thresholdType,
            Long minGapOfThresholds) {

        return new NeedReportToCoordinatorProperties(minDatapoints, thresholdType, minGapOfThresholds);

    }
}
