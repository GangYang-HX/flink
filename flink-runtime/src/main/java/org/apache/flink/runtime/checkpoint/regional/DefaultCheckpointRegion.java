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

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link CheckpointRegion}.
 */
public class DefaultCheckpointRegion implements CheckpointRegion {

    private final Set<ExecutionAttemptID> executionAttempts = new HashSet<>();

    private final Set<OperatorInstanceID> operatorInstances = new HashSet<>();

    private final Map<ExecutionAttemptID, Set<OperatorInstanceID>> operatorInstancesByExecutionAttempt = new HashMap<>();

    private final Map<OperatorInstanceID, ExecutionVertex> operatorInstanceId2ExecutionVertex = new HashMap<>();

    private final Map<ExecutionAttemptID, ExecutionVertex> executionAttemptId2ExecutionVertex = new HashMap<>();

    @Override
    public Set<ExecutionAttemptID> getAllExecutionAttempts() {
        return executionAttempts;
    }

    @Override
    public Set<OperatorInstanceID> getAllOperatorInstances() {
        return operatorInstances;
    }

    @Override
    public Set<OperatorInstanceID> getOperatorInstances(ExecutionAttemptID executionAttempt) {
        Preconditions.checkState(operatorInstancesByExecutionAttempt.containsKey(executionAttempt));

        return operatorInstancesByExecutionAttempt.get(executionAttempt);
    }

    @Override
    public void add(ExecutionAttemptID executionAttempt, ExecutionVertex executionVertex) {
        int subtaskIndex = executionVertex.getParallelSubtaskIndex();
        List<OperatorIDPair> operatorIDPairs = executionVertex.getJobVertex().getOperatorIDs();
        Set<OperatorInstanceID> operatorInstancesToAdd = operatorIDPairs.stream()
                .map(oip -> oip.getUserDefinedOperatorID().orElseGet(oip::getGeneratedOperatorID))
                .map(oi -> new OperatorInstanceID(subtaskIndex, oi))
                .collect(Collectors.toSet());
        executionAttempts.add(executionAttempt);
        operatorInstances.addAll(operatorInstancesToAdd);
        operatorInstancesByExecutionAttempt.put(executionAttempt, operatorInstancesToAdd);
        operatorInstancesToAdd.forEach(oi -> operatorInstanceId2ExecutionVertex.put(oi, executionVertex));
        executionAttemptId2ExecutionVertex.put(executionAttempt, executionVertex);
    }

    @Override
    public ExecutionVertex getExecutionVertex(OperatorInstanceID operatorInstanceId) {
        return this.operatorInstanceId2ExecutionVertex.get(operatorInstanceId);
    }

    @Override
    public ExecutionVertex getExecutionVertex(ExecutionAttemptID executionAttemptId) {
        return this.executionAttemptId2ExecutionVertex.get(executionAttemptId);
    }

    @Override
    public String toString() {
        return "DefaultCheckpointRegion{" +
                executionAttempts.size() + " ExecutionAttempt's, " +
                operatorInstances.size() + " OperatorInstance's" +
                '}';
    }
}
