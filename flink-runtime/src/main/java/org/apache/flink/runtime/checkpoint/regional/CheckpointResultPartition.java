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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Case class containing the necessary fields for building {@link DefaultCheckpointTopology}.
 */
public class CheckpointResultPartition {

    private final IntermediateResultPartitionID resultPartitionId;

    private final IntermediateDataSetID intermediateDataSetId;

    private final ResultPartitionType partitionType;

    private final Supplier<ResultPartitionState> resultPartitionStateSupplier;

    private CheckpointVertex producer;

    private final List<CheckpointVertex> consumers;

    public CheckpointResultPartition(
            IntermediateResultPartitionID partitionId,
            IntermediateDataSetID intermediateDataSetId,
            ResultPartitionType partitionType,
            Supplier<ResultPartitionState> resultPartitionStateSupplier) {
        this.resultPartitionId = checkNotNull(partitionId);
        this.intermediateDataSetId = checkNotNull(intermediateDataSetId);
        this.partitionType = checkNotNull(partitionType);
        this.resultPartitionStateSupplier = checkNotNull(resultPartitionStateSupplier);
        this.consumers = new ArrayList<>();
    }

    public IntermediateResultPartitionID getId() {
        return resultPartitionId;
    }

    public IntermediateDataSetID getResultId() {
        return intermediateDataSetId;
    }

    public ResultPartitionType getResultType() {
        return partitionType;
    }

    public ResultPartitionState getState() {
        return resultPartitionStateSupplier.get();
    }

    public CheckpointVertex getProducer() {
        return producer;
    }

    public Iterable<CheckpointVertex> getConsumers() {
        return consumers;
    }

    public void addConsumer(CheckpointVertex vertex) {
        consumers.add(checkNotNull(vertex));
    }

    public void setProducer(CheckpointVertex vertex) {
        producer = checkNotNull(vertex);
    }
}
