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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.persistence.filesystem.FileSystemReaderWriterHelper;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;

import java.util.Collection;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** CheckpointRecoveryFactory used for FileSystem. */
public class FileSystemCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

    private static final String CHECKPOINTS_PATH = "/checkpoints";

    /** The FileSystem to use. */
    private final FileSystem fs;

    private final Configuration config;

    private final Executor executor;

    public FileSystemCheckpointRecoveryFactory(
            FileSystem fs, Configuration config, Executor executor) {
        this.fs = checkNotNull(fs, "FileSystem");
        this.config = checkNotNull(config, "Configuration");
        this.executor = checkNotNull(executor, "Executor");
    }

    @Override
    public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId,
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            RestoreMode restoreMode)
            throws Exception {
        FileSystemReaderWriterHelper<CompletedCheckpoint> readerWriterHelper =
                new FileSystemReaderWriterHelper(fs, config);

        Path pathPrefix =
                new Path(
                        new Path(
                                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                        config),
                                CHECKPOINTS_PATH),
                        jobId.toString());

        Collection<CompletedCheckpoint> completedCheckpoints =
                FileSystemCompletedCheckpointStoreUtils.retrieveCompletedCheckpoint(
                        fs, readerWriterHelper, pathPrefix);

        return new FileSystemCompletedCheckpointStore(
                fs,
                readerWriterHelper,
                pathPrefix,
                maxNumberOfCheckpointsToRetain,
                completedCheckpoints,
                sharedStateRegistryFactory.create(ioExecutor, completedCheckpoints, restoreMode),
                executor);
    }

    @Override
    public CheckpointIDCounter createCheckpointIDCounter(JobID jobID) throws Exception {
        return new FileSystemCheckpointIDCounter(fs, config, jobID);
    }
}
