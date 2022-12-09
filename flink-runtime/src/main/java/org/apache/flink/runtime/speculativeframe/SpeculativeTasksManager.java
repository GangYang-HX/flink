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

/**
 * Manager for speculative tasks. The implementation will be instantiated and executed
 * on task manager. There is a corresponding coordinator which coordinates multiple
 * {@link SpeculativeTasksManager}s. For tasks whose scope is TaskManager, the
 * implementation will also maintain the statistics for them.
 */
public interface SpeculativeTasksManager {

    //---------------------------------------------------------------------------------------------
    // Interfaces to set options for the tasks.
    //---------------------------------------------------------------------------------------------

    SpeculativeTasksManager registerTaskType(SpeculativeProperties properties);

    /**
     * Start the {@link SpeculativeTasksManager}. The report of the configurations (to the
     * coordinator) can be done in this method. If no options are set for a type of tasks,
     * the coordinator will initialize all the options to the default value when the first
     * data is reported.
     */
    void start();

    /**
     * Submit a task to {@link SpeculativeTasksManager} for speculating.
     * {@link SpeculativeTasksManager} should take over the task, and the interaction
     * should only be taken by the user-supplied callbacks.
     *
     * @param taskInfo {@link SpeculativeTaskInfo} containing the task information.
     */
    void submitTask(SpeculativeTaskInfo taskInfo);

    /**
     * Notify the implementation that the threshold has changed. This may be called from
     * a different thread.
     *
     * @param taskType identifying the type of task.
     * @param threshold the new (or first) threshold for the type of task.
     */
    void notifyThreshold(String taskType, long threshold);


    /**
     * Shutdown speculative manager. We can release all resources here.
     */
    void shutdown();
}
