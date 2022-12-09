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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

/**
 * This class is used to constract a special {@link RocksDBStateBackend}
 * which don't need to persist file to underling file system.
 */
public class DummyPersistStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) {
        throw new UnsupportedOperationException("This class don't support");
    }

    @Override
    public CheckpointStorage createCheckpointStorage(JobID jobId) {
        return new DummyPersistCheckpointStorage();
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) throws
            IOException {
        throw new UnsupportedOperationException("This class don't support");
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        throw new UnsupportedOperationException("This class don't support");
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
        return this;
    }

    private static class DummyPersistCheckpointStorage implements CheckpointStorage {

        @Override
        public boolean supportsHighlyAvailableStorage() {
            return false;
        }

        @Override
        public boolean hasDefaultSavepointLocation() {
            return false;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) {
            throw new UnsupportedOperationException("This class don't support");
        }

        @Override
        public void initializeBaseLocations() {}

        @Override
        public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) {
            throw new UnsupportedOperationException("This class don't support");
        }

        @Override
        public CheckpointStorageLocation initializeLocationForSavepoint(
                long checkpointId,
                @Nullable String externalLocationPointer) {
            throw new UnsupportedOperationException("This class don't support");
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId,
                CheckpointStorageLocationReference reference) {
            throw new UnsupportedOperationException("This class don't support");
        }

        @Override
        public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() {
            throw new UnsupportedOperationException("This class don't support");
        }
    }
}
