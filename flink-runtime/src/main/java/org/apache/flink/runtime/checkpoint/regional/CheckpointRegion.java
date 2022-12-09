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

package org.apache.flink.runtime.checkpoint.regional;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.topology.PipelinedRegion;

import java.util.Set;

/**
 * Interface for group operators in sub-task level. This interface contains almost the same {@link ExecutionVertex}s
 * as {@link PipelinedRegion}. We create this instead of reusing {@link PipelinedRegion} because we need the operator
 * information instead of the {@link ExecutionVertex}s, which is not provided by {@link PipelinedRegion}.
 */
public interface CheckpointRegion {

    /**
     * Get all the execution attempts for tasks that are make up by operators belonging to this region.
     * This is used to find checkpoint related messages.
     */
    Set<ExecutionAttemptID> getAllExecutionAttempts();

    /**
     * Get all the OperatorID + subTaskIndex pairs that belong to this region.
     * This is used to find all the {@link OperatorSubtaskState}s that need to reference
     * previous successful checkpoint when any ExecutionAttemptIDs belonging to
     * this region fail.
     */
    Set<OperatorInstanceID> getAllOperatorInstances();

    /**
     * Get the OperatorID + subTaskIndex pairs that belong to the given {@link ExecutionAttemptID}.
     */
    Set<OperatorInstanceID> getOperatorInstances(ExecutionAttemptID executionAttempt);

    /**
     * Add a {@link ExecutionAttemptID} and its corresponding {@link OperatorInstanceID}s to this region.
     */
    void add(ExecutionAttemptID executionAttempt, ExecutionVertex executionVertex);

    /**
     * Get the {@link ExecutionVertex} that the given {@link OperatorInstanceID} belongs to.
     */
    ExecutionVertex getExecutionVertex(OperatorInstanceID operatorInstanceId);

    /**
     * Get the {@link ExecutionVertex} corresponding to the given {@link ExecutionAttemptID} in this region.
     */
    ExecutionVertex getExecutionVertex(ExecutionAttemptID executionAttemptId);
}
