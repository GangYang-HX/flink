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

import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

public final class CheckpointRegionComputeUtil {

    public static Set<Set<CheckpointVertex>> computeCheckpointRegion(
            DefaultCheckpointTopology topology,
            boolean excludeBroadcastedVertex) {

        final Map<CheckpointVertex, Set<CheckpointVertex>> vertexToRegion = new IdentityHashMap<>();

        for (CheckpointVertex vertex : topology.getExecutionVertices()) {
            Set<CheckpointVertex> currentRegion = new HashSet<>();
            currentRegion.add(vertex);
            vertexToRegion.put(vertex, currentRegion);

            for (CheckpointResultPartition consumedResult : vertex.getConsumedResults()) {
                JobVertex jobVertex = vertex.getJobVertex();
                JobVertex producerJobVertex = consumedResult.getProducer().getJobVertex();
                boolean skipBroadcastedUpstream =
                        excludeBroadcastedVertex &&
                                (jobVertex.getBroadcastedUpstreams() != null) &&
                                jobVertex.getBroadcastedUpstreams().contains(producerJobVertex);
                if (consumedResult.getResultType().isPipelined() && !skipBroadcastedUpstream) {
                    final CheckpointVertex producerVertex = consumedResult.getProducer();
                    final Set<CheckpointVertex> producerRegion = vertexToRegion.get(producerVertex);

                    if (producerRegion == null) {
                        throw new IllegalStateException(
                                "Checkpoint region containing the producer task  is null while calculating " +
                                        "checkpoint region for the consumer task. This should be a checkpoint " +
                                        "region building bug.");
                    }

                    // Check if it is the same as the producer checkpoint region, if so skip the merge.
                    // This check can significantly reduce compute complexity in the All-to-All PIPELINED case.
                    if (currentRegion != producerRegion) {
                        // Merge current checkpoint region and the producer checkpoint region.
                        // To reduce the cost, merge the smaller region into the larger one.
                        final Set<CheckpointVertex> smallerSet;
                        final Set<CheckpointVertex> largerSet;
                        if (currentRegion.size() < producerRegion.size()) {
                            smallerSet = currentRegion;
                            largerSet = producerRegion;
                        } else {
                            smallerSet = producerRegion;
                            largerSet = currentRegion;
                        }
                        for (CheckpointVertex v : smallerSet) {
                            vertexToRegion.put(v, largerSet);
                        }
                        largerSet.addAll(smallerSet);
                        currentRegion = largerSet;
                    }
                }
            }
        }

        return uniqueRegions(vertexToRegion);
    }

    private static Set<Set<CheckpointVertex>> uniqueRegions(
            final Map<CheckpointVertex, Set<CheckpointVertex>> vertexToRegion) {
        final Set<Set<CheckpointVertex>> distinctRegions = Collections.newSetFromMap(new IdentityHashMap<>());
        distinctRegions.addAll(vertexToRegion.values());
        return distinctRegions;
    }
}
