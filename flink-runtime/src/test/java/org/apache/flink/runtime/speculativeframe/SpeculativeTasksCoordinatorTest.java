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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.speculativeframe.rpc.NeedReportToCoordinatorProperties;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTaskTimeCosts;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTasksOptions;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpeculativeTasksCoordinatorTest {

    SpeculativeTasksCoordinator coordinator = new SpeculativeTasksCoordinator();

    List<Tuple2<String, NeedReportToCoordinatorProperties>> options1 = new ArrayList<>();

    List<Tuple2<String, NeedReportToCoordinatorProperties>> options2 = new ArrayList<>();

    List<Tuple2<String, Collection<Long>>> timeCosts = new ArrayList<>();

    @Before
    public void init() {
        options1.add(Tuple2.of("UUID1", NeedReportToCoordinatorProperties.of(1, SpeculativeThresholdType.NINETY, 1000L)));
        options1.add(Tuple2.of("UUID2", NeedReportToCoordinatorProperties.of(2, SpeculativeThresholdType.FIFTY, 1000L)));
        options2.add(Tuple2.of("UUID1", NeedReportToCoordinatorProperties.of(1, SpeculativeThresholdType.NINETY, 1000L)));
        options2.add(Tuple2.of("UUID3", NeedReportToCoordinatorProperties.of(3, SpeculativeThresholdType.NINETY_FIVE, 1000L)));

        List<Long> times = new ArrayList<>();
        times.add(1000L);
        times.add(2000L);
        times.add(2000L);
        times.add(1000L);
        timeCosts.add(Tuple2.of("UUID1", times));
    }

    @Test
    public void testLoadSpeculativeTaskOptions() throws ExecutionException, InterruptedException {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            coordinator.addSpeculativeTaskOptions(new SpeculativeTasksOptions(options1, "TaskManager-1"));
        }));

        futures.add(CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            coordinator.addSpeculativeTaskOptions(new SpeculativeTasksOptions(options2, "TaskManager-2"));
        }));

        FutureUtils.waitForAll(futures).get();
        Map<String, NeedReportToCoordinatorProperties> options = coordinator.getOptions();

        assertEquals(3, options.size());
        assertTrue(options.containsKey("UUID1"));
        assertTrue(options.containsKey("UUID2"));
        assertTrue(options.containsKey("UUID3"));
    }

    @Test
    public void testReceivedTaskTimeCosts() {
        coordinator.addSpeculativeTaskOptions(new SpeculativeTasksOptions(options1, "TaskManager-1"));
        coordinator.receivedTaskTimeCosts(new SpeculativeTaskTimeCosts(timeCosts));
        Histogram histogram = coordinator.getStatistics().get("UUID1");
        assertEquals(4, histogram.getCount());
        assertEquals(1500.0, histogram.getStatistics().getQuantile(SpeculativeThresholdType.FIFTY.getQuantile()), 1);
    }
}
