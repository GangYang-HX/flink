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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.speculativeframe.SpeculativeProperties;
import org.apache.flink.runtime.speculativeframe.SpeculativeTaskInfo;
import org.apache.flink.runtime.speculativeframe.SpeculativeTasksManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestSpeculativeManager implements SpeculativeTasksManager {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Override
    public SpeculativeTasksManager registerTaskType(SpeculativeProperties properties) {
        return this;
    }

    @Override
    public void start() {

    }

    @Override
    public void submitTask(SpeculativeTaskInfo taskInfo) {
        Future<?> future = executorService.submit(() -> taskInfo.getTask().apply(null));
        try {
            taskInfo.getSucceedCallback().accept(future.get());
        } catch (Exception e) {
            taskInfo.getFailCallback().accept(
                    new SpeculativeTaskInfo.TaskFailureInfo(false, null, false, false, null, e));
        }
    }

    @Override
    public void notifyThreshold(String taskType, long threshold) {

    }

    @Override
    public void shutdown() {

    }
}
