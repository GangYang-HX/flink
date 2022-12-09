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

import org.apache.flink.util.Preconditions;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SpeculativeProperties {

    private final String speculativeTaskType;

    private final Optional<ExecutorService> executor;

    private final Optional<Integer> minNumDataPoints;

    private final Optional<SpeculativeScope> scope;

    private final Optional<SpeculativeThresholdType> thresholdType;

    private final Optional<Long> minThreshold;

    private final Optional<Long> minGapBetweenThresholdReports;

    private SpeculativeProperties(
            String speculativeTaskType,
            Optional<ExecutorService> executor,
            Optional<Integer> minNumDataPoints,
            Optional<SpeculativeScope> scope,
            Optional<SpeculativeThresholdType> thresholdType,
            Optional<Long> minThreshold,
            Optional<Long> minGapBetweenThresholdReports) {
        this.speculativeTaskType = speculativeTaskType;
        this.executor = executor;
        this.minNumDataPoints = minNumDataPoints;
        this.scope = scope;
        this.thresholdType = thresholdType;
        this.minThreshold = minThreshold;
        this.minGapBetweenThresholdReports = minGapBetweenThresholdReports;
    }

    public String getSpeculativeTaskType() {
        return speculativeTaskType;
    }

    public Optional<ExecutorService> getExecutor() {
        return executor;
    }

    public Optional<Integer> getMinNumDataPoints() {
        return minNumDataPoints;
    }

    public Optional<SpeculativeScope> getScope() {
        return scope;
    }

    public Optional<SpeculativeThresholdType> getThresholdType() {
        return thresholdType;
    }

    public Optional<Long> getMinThreshold() {
        return minThreshold;
    }

    public Optional<Long> getMinGapBetweenThresholdReports() {
        return minGapBetweenThresholdReports;
    }

    public static class SpeculativePropertiesBuilder {
        private final String speculativeTaskType;
        private Optional<ExecutorService> executor;
        private Optional<Integer> minNumDataPoints;
        private Optional<SpeculativeScope> scope;
        private Optional<SpeculativeThresholdType> thresholdType;
        private Optional<Long> customMinThreshold;
        private Optional<Long> customMinGapThreshold;

        public SpeculativePropertiesBuilder(String speculativeTaskType) {
            this.speculativeTaskType = speculativeTaskType;
            this.executor = Optional.empty();
            this.minNumDataPoints = Optional.empty();
            this.scope = Optional.empty();
            this.thresholdType = Optional.empty();
            this.customMinThreshold = Optional.empty();
            this.customMinGapThreshold = Optional.empty();
        }

        public SpeculativePropertiesBuilder setExecutor(ExecutorService executor) {
            Preconditions.checkNotNull(executor);
            this.executor = Optional.of(executor);
            return this;
        }

        public SpeculativePropertiesBuilder setMinNumDataPoints(Integer minNumDataPoints) {
            Preconditions.checkNotNull(minNumDataPoints);
            this.minNumDataPoints = Optional.of(minNumDataPoints);
            return this;
        }

        public SpeculativePropertiesBuilder setScope(SpeculativeScope scope) {
            Preconditions.checkNotNull(scope);
            this.scope = Optional.of(scope);
            return this;
        }

        public SpeculativePropertiesBuilder setThresholdType(SpeculativeThresholdType thresholdType) {
            Preconditions.checkNotNull(thresholdType);
            this.thresholdType = Optional.of(thresholdType);
            return this;
        }

        public SpeculativePropertiesBuilder setCustomMinThreshold(Long customMinThreshold) {
            Preconditions.checkNotNull(thresholdType);
            this.customMinThreshold = Optional.of(customMinThreshold);
            return this;
        }

        public SpeculativePropertiesBuilder setCustomMinGapThreshold(Long customMinGapThreshold) {
            Preconditions.checkNotNull(customMinGapThreshold);
            this.customMinGapThreshold = Optional.of(customMinGapThreshold);
            return this;
        }

        public SpeculativeProperties build() {
            return new SpeculativeProperties(
                speculativeTaskType,
                executor,
                minNumDataPoints,
                scope,
                thresholdType,
                customMinThreshold,
                customMinGapThreshold);
        }
    }
}
