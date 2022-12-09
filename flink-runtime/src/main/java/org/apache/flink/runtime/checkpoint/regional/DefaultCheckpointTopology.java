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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link CheckpointTopology}.
 */
public class DefaultCheckpointTopology implements CheckpointTopology {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCheckpointTopology.class);

    private final Map<ExecutionAttemptID, DefaultCheckpointRegion> executionAttempt2CheckpointRegion;

    private final Map<CheckpointVertex, ExecutionAttemptID> checkpointVertex2ExecutionAttempt;

    private final List<CheckpointVertex> executionVertices;

    public DefaultCheckpointTopology(ExecutionGraph graph, boolean excludeBroadcastedVertex) {
        Preconditions.checkNotNull(graph, "Execution graph should not be null");

        Map<ExecutionAttemptID, Execution> executions = graph.getRegisteredExecutions();
        Map<ExecutionVertex, ExecutionAttemptID> executionVertex2ExecutionAttempt = new HashMap<>();
        for (ExecutionAttemptID executionAttempt : executions.keySet()) {
            Execution execution = executions.get(executionAttempt);
            ExecutionVertex vertex = execution.getVertex();
            executionVertex2ExecutionAttempt.put(vertex, executionAttempt);
        }

        this.executionAttempt2CheckpointRegion = new HashMap<>();
        this.executionVertices = new ArrayList<>(graph.getTotalNumberOfVertices());
        this.checkpointVertex2ExecutionAttempt = new HashMap<>();

        Map<ExecutionVertex, CheckpointVertex> executionVertexMap = new HashMap<>();
        Map<IntermediateResultPartitionID, CheckpointResultPartition>  tmpResultPartitionsById = new HashMap<>();
        for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
            List<CheckpointResultPartition> producedPartitions =
                    generateProducedCheckpointResultPartition(vertex.getProducedPartitions());

            producedPartitions.forEach(partition -> tmpResultPartitionsById.put(partition.getId(), partition));

            CheckpointVertex checkpointVertex = generateCheckpointVertex(vertex, producedPartitions);
            this.executionVertices.add(checkpointVertex);
            executionVertexMap.put(vertex, checkpointVertex);
            this.checkpointVertex2ExecutionAttempt.put(
                    checkpointVertex,
                    executionVertex2ExecutionAttempt.get(vertex));
        }

        connectVerticesToConsumedPartitions(executionVertexMap, tmpResultPartitionsById);

        initializeCheckpointRegions(excludeBroadcastedVertex);
    }

    @Override
    public CheckpointRegion getCheckpointRegion(ExecutionAttemptID executionAttempt) {
        Preconditions.checkNotNull(executionAttempt2CheckpointRegion);

        return executionAttempt2CheckpointRegion.get(executionAttempt);
    }

    public Map<ExecutionAttemptID, DefaultCheckpointRegion> getExecutionAttempt2CheckpointRegion() {
        return executionAttempt2CheckpointRegion;
    }

    public Set<CheckpointRegion> getCheckpointRegions() {
        return executionAttempt2CheckpointRegion.values().stream()
                .map(cr -> ((CheckpointRegion) cr))
                .collect(Collectors.toSet());
    }

    public List<CheckpointVertex> getExecutionVertices() {
        return executionVertices;
    }

    private List<CheckpointResultPartition> generateProducedCheckpointResultPartition(
            Map<IntermediateResultPartitionID, IntermediateResultPartition> producedIntermediatePartitions) {

        List<CheckpointResultPartition> producedSchedulingPartitions =
                new ArrayList<>(producedIntermediatePartitions.size());

        producedIntermediatePartitions.values().forEach(
                irp -> producedSchedulingPartitions.add(
                        new CheckpointResultPartition(
                                irp.getPartitionId(),
                                irp.getIntermediateResult().getId(),
                                irp.getResultType(),
                                () -> irp.isConsumable() ? ResultPartitionState.CONSUMABLE : ResultPartitionState.CREATED)));

        return producedSchedulingPartitions;
    }

    private CheckpointVertex generateCheckpointVertex(
            ExecutionVertex vertex,
            List<CheckpointResultPartition> producedPartitions) {

        CheckpointVertex schedulingVertex = new CheckpointVertex(
                vertex,
                producedPartitions);
        producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));

        return schedulingVertex;
    }

    private void connectVerticesToConsumedPartitions(
            Map<ExecutionVertex, CheckpointVertex> executionVertexMap,
            Map<IntermediateResultPartitionID, CheckpointResultPartition> resultPartitions) {

        for (Map.Entry<ExecutionVertex, CheckpointVertex> mapEntry : executionVertexMap.entrySet()) {
            final CheckpointVertex checkpointVertex = mapEntry.getValue();
            final ExecutionVertex executionVertex = mapEntry.getKey();

            for (int index = 0; index < executionVertex.getNumberOfInputs(); index++) {
                for (ExecutionEdge edge : executionVertex.getInputEdges(index)) {
                    CheckpointResultPartition partition = resultPartitions.get(edge.getSource().getPartitionId());
                    checkpointVertex.addConsumedResult(partition);
                    partition.addConsumer(checkpointVertex);
                }
            }
        }
    }

    private void initializeCheckpointRegions(boolean excludeBroadcastedVertex) {
        Preconditions.checkNotNull(checkpointVertex2ExecutionAttempt);
        Preconditions.checkNotNull(executionAttempt2CheckpointRegion);

        long startTime = System.nanoTime();
        Set<Set<CheckpointVertex>> regions =
                CheckpointRegionComputeUtil.computeCheckpointRegion(this, excludeBroadcastedVertex);
        for (Set<CheckpointVertex> region : regions) {
            DefaultCheckpointRegion checkpointRegion = new DefaultCheckpointRegion();
            for (CheckpointVertex vertex : region) {
                ExecutionAttemptID executionAttempt = checkpointVertex2ExecutionAttempt.get(vertex);
                ExecutionVertex executionVertex = vertex.getExecutionVertex();
                checkpointRegion.add(executionAttempt, executionVertex);
                executionAttempt2CheckpointRegion.put(executionAttempt, checkpointRegion);
            }
        }
        long timeTaken = System.nanoTime() - startTime;
        LOG.info("It took " + timeTaken + " ns to build the CheckpointTopology");
    }
}
