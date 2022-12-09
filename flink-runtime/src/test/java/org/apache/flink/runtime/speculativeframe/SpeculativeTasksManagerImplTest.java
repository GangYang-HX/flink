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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.speculativeframe.rpc.NeedReportToCoordinatorProperties;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTasksOptions;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(Theories.class)
public class SpeculativeTasksManagerImplTest {

    SpeculativeTasksManagerImpl speculativeTasksManager;
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();

    @Before
    public void init() {
        JobMasterGateway mockJobMaster = mock(JobMasterGateway.class);

        speculativeTasksManager = new SpeculativeTasksManagerImpl(
                mockJobMaster,
                "TaskManager-1",
                2,
                100,
                1,
                3000,
                new UnregisteredMetricsGroup());
        speculativeTasksManager.start();
        ExecutorService service = Executors.newFixedThreadPool(2);
        SpeculativeProperties properties1 = new SpeculativeProperties.SpeculativePropertiesBuilder(uuid1)
                .setScope(SpeculativeScope.TaskManager)
                .setMinNumDataPoints(10)
                .setThresholdType(SpeculativeThresholdType.FIFTY)
                .setExecutor(service)
                .build();

        SpeculativeProperties properties2 = new SpeculativeProperties.SpeculativePropertiesBuilder(uuid2)
                .setScope(SpeculativeScope.JobManager)
                .setMinNumDataPoints(5)
                .setThresholdType(SpeculativeThresholdType.NINETY)
                .setExecutor(service)
                .build();

        speculativeTasksManager.registerTaskType(properties1)
                .registerTaskType(properties2);
    }

    @Test
    public void testReportOptions() throws InterruptedException {
        JobMasterGateway jm = speculativeTasksManager.getJobMaster();
        ArgumentCaptor<SpeculativeTasksOptions> reportArgument = ArgumentCaptor.forClass(SpeculativeTasksOptions.class);

        SpeculativeTaskInfo taskInfo = new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
                uuid2,
                taskData -> {
                    try {
                        // mock run
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return uuid2;
                })
                .build();
        speculativeTasksManager.submitTask(taskInfo);
        Thread.sleep(2000L);

        verify(jm).reportSpeculativeTaskOptions(reportArgument.capture());
        SpeculativeTasksOptions speculativeTasksOptions = reportArgument.getValue();

        assertNotNull(speculativeTasksOptions);

        Collection<Tuple2<String, NeedReportToCoordinatorProperties>> options = speculativeTasksOptions.getOptions();

        assertEquals(1, options.size());

        options.forEach(stringTuple2Tuple2 -> {
            Integer minDataPoints = stringTuple2Tuple2.f1.getMinDatapoints();
            String thresoldType = stringTuple2Tuple2.f1.getThresholdType().name();
            if (stringTuple2Tuple2.f0.equals(uuid1)) {
                assertEquals(10, (int) minDataPoints);
                assertEquals(SpeculativeThresholdType.FIFTY.name(), thresoldType);
            } else if (stringTuple2Tuple2.f0.equals(uuid2)) {
                assertEquals(5, (int) minDataPoints);
                assertEquals(SpeculativeThresholdType.NINETY.name(), thresoldType);
            } else {
                fail();
            }
        });
    }


    @Test
    public void testNotifyThresholdsLocally() {

        speculativeTasksManager.notifyThreshold(uuid1, 1000);

        assertEquals(1, speculativeTasksManager.getCurrentThresholds().size());
        assertEquals(1000, (long) speculativeTasksManager.getCurrentThresholds().get(uuid1));
    }

    @Test
    public void testSubmit() throws InterruptedException {
        SpeculativeTaskInfo taskInfo = new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
                uuid1,
                taskData -> {
                    try {
                        // mock run
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return uuid1;
                })
                .build();
        speculativeTasksManager.submitTask(taskInfo);

        Thread.sleep(100L);

        assertTrue(taskInfo.getSubmittedTime() > 0);
    }


    @Test
    public void testSpeculativeExecute() throws InterruptedException {
        speculativeTasksManager.notifyThreshold(uuid1, 500);
        SpeculativeTaskInfo taskInfo = new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
                uuid1,
                taskData -> {
                    try {
                        // mock run
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                    }
                    return uuid1;
                })
                .build();
        speculativeTasksManager.submitTask(taskInfo);
        Thread.sleep(2000L);

        assertTrue(taskInfo.isFinished());
        assertTrue(taskInfo.isSpeculatedTaskSubmitted());
        assertTrue(taskInfo.isOriginalTaskSucceeded());
        assertFalse(taskInfo.isSpeculativeTaskSucceeded());

    }

    public static @DataPoints boolean[] args = {true, false};

    @Theory
    public void testCallback(boolean canRespondInterrupt) throws InterruptedException {
        boolean[] callbackFlag = new boolean[]{false, false, false, false};
        int[] callTimes = new int[4];

        speculativeTasksManager.notifyThreshold(uuid1, 500);
        AtomicLong sleepMs = new AtomicLong(1000);
        SpeculativeTaskInfo taskInfo = new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
                uuid1,
                taskData -> {
                    try {
                        // mock run
                        Thread.sleep((Long) taskData);
                    } catch (InterruptedException e) {
                        if (canRespondInterrupt) {
                            throw new RuntimeException(e);
                        }
                    }
                    return uuid1;
                })
                .setTaskDataGenerator(() -> sleepMs.getAndAdd(1000))
                .setFailCallback(taskFailureInfo -> {
                    callbackFlag[0] = true;
                    callTimes[0] = callTimes[0] + 1;
                })
                .setSucceedCallback( succeed -> {
                    callbackFlag[1] = true;
                    callTimes[1] = callTimes[1] + 1;
                })
                .setTaskCanceller(o -> {
                    callbackFlag[2] = true;
                    callTimes[2] = callTimes[2] + 1;
                })
                .setDuplicatedTaskSucceedsCallback(result -> {
                    callbackFlag[3] = true;
                    callTimes[3] = callTimes[3] + 1;
                })
                .build();
        speculativeTasksManager.submitTask(taskInfo);
        Thread.sleep(3000L);


        assertTrue(taskInfo.isFinished());
        assertTrue(taskInfo.isOriginalTaskSucceeded());
        assertFalse(taskInfo.isSpeculativeTaskSucceeded());
        assertTrue(taskInfo.getSpeculatedTask().cancelled);
        // check
        if (canRespondInterrupt) {
            assertTrue(callbackFlag[0] && callbackFlag[1] && callbackFlag[2] && !callbackFlag[3]);
            assertEquals(1, callTimes[0]);
            assertEquals(1, callTimes[1]);
            assertEquals(1, callTimes[2]);
            assertEquals(0, callTimes[3]);
        } else {
            assertTrue(!callbackFlag[0] && callbackFlag[1] && callbackFlag[2] && callbackFlag[3]);
            assertEquals(0, callTimes[0]);
            assertEquals(1, callTimes[1]);
            assertEquals(1, callTimes[2]);
            assertEquals(1, callTimes[3]);
        }
    }


}
