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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.speculativeframe.rpc.NeedReportToCoordinatorProperties;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTaskTimeCosts;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTasksOptions;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashMultimap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.speculativeframe.Constants.HISTOGRAM_WINDOW_SIZE;

/**
 * Coordinator for speculative tasks. The implementation will be instantiated and executed on the
 * job manager. The implementation is responsible for maintaining configurations and statistics
 * for tasks scoped by {@link SpeculativeScope#JobManager}.
 */
public class SpeculativeTasksCoordinator {

    private final Map<String, NeedReportToCoordinatorProperties> options;

    // <TaskManagerId, List<taskType>>
    private final Multimap<String, String> taskTypesOfTaskmanagers;

    private final Map<String, Histogram> statistics;

    private final Map<String, Long> lastNotifyThresholds;


    public SpeculativeTasksCoordinator() {
        this.statistics = new ConcurrentHashMap<>();
        this.options = new HashMap<>();
        this.taskTypesOfTaskmanagers = HashMultimap.create();
        this.lastNotifyThresholds = new ConcurrentHashMap<>();
    }

    public void addSpeculativeTaskOptions(SpeculativeTasksOptions options) {
        // All taskmanagers will report their options, we need to deduplicate.
        synchronized (this.options) {
            options.getOptions()
                    .forEach(entry -> {
                        this.options.put(
                                entry.f0,
                                NeedReportToCoordinatorProperties.of(
                                        entry.f1.getMinDatapoints(),
                                        entry.f1.getThresholdType(),
                                        entry.f1.getMinGapOfThresholds()));
                        taskTypesOfTaskmanagers.put(options.getTaskManagerId(), entry.f0);
                    });
        }
    }

    public void receivedTaskTimeCosts(SpeculativeTaskTimeCosts timeCosts) {
        timeCosts.getTimeCosts()
                .forEach(costInfo -> {
                    Histogram histogram = statistics.computeIfAbsent(
                            costInfo.f0,
                            x -> new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
                    costInfo.f1.forEach(histogram::update);
                });
    }

    public Multimap<String, String> getTaskTypesOfTaskmanagers() {
        return taskTypesOfTaskmanagers;
    }

    public Collection<Tuple2<String, Long>> getThresholdsWithTaskmanager(String taskManagerId) {
        List<Tuple2<String, Long>> thresholds = new ArrayList<>();
        taskTypesOfTaskmanagers.get(taskManagerId)
                .forEach(taskType -> {
                    Histogram histogram = statistics.get(taskType);
                    NeedReportToCoordinatorProperties properties = options.get(taskType);
                    if (histogram.getCount() >= properties.getMinDatapoints()) {
                        long value = (long) histogram.getStatistics()
                                .getQuantile(properties.getThresholdType().getQuantile());
                        if (shouldNotify(taskType, value)) {
                            // should notify threshold
                            thresholds.add(Tuple2.of(taskType, value));
                            updateLastNotifyThreshold(taskType, value);
                        }
                    }
                });
        return thresholds;
    }

    private void updateLastNotifyThreshold(String taskType, long value) {
        lastNotifyThresholds.put(taskType, value);
    }

    private boolean shouldNotify(String taskType, long value) {
        if (null == lastNotifyThresholds.get(taskType)) {
            return true;
        }
        return Math.abs(lastNotifyThresholds.get(taskType) - value) > options.get(taskType).getMinGapOfThresholds();
    }

    @VisibleForTesting
    Map<String, NeedReportToCoordinatorProperties> getOptions() {
        return options;
    }

    @VisibleForTesting
    Map<String, Histogram> getStatistics() {
        return statistics;
    }
}
