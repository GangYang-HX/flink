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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.filesystem.FileSystemReaderWriterHelper;
import org.apache.flink.runtime.state.SharedStateRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers with the FileSystem implement.
 *
 * <p>Each checkpoint creates a FileSystem path:
 *
 * <pre>
 * +----O /flink/cluster-id/checkpoints/&lt;job-id&gt;/&lt;checkpoint-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/cluster-id/checkpoints/&lt;job-id&gt;/&lt;checkpoint-id&gt; N [persistent]
 * </pre>
 *
 * <p>During recovery, the latest checkpoint is read from a FileSystem. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number of retained
 * checkpoints is greater than one).
 */
public class FileSystemCompletedCheckpointStore extends AbstractCompleteCheckpointStore
        implements CompletedCheckpointStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(FileSystemCompletedCheckpointStore.class);

    private final Executor executor;

    private final Path pathPrefix;

    /** The maximum number of checkpoints to retain (at least 1). */
    private final int maxNumberOfCheckpointsToRetain;

    /**
     * Local copy of the completed checkpoints in FileSystem. This is restored from FileSystem when
     * recovering and is maintained in parallel to the state in FileSystem during normal operations.
     */
    private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    /** The FileSystem. */
    private final FileSystem fs;

    /** The FileSystem helper to write and read CompletedCheckpoint. */
    private final FileSystemReaderWriterHelper<CompletedCheckpoint> readerWriterHelper;

    public FileSystemCompletedCheckpointStore(
            FileSystem fs,
            FileSystemReaderWriterHelper<CompletedCheckpoint> readerWriterHelper,
            Path pathPrefix,
            int maxNumberOfCheckpointsToRetain,
            Collection<CompletedCheckpoint> completedCheckpoints,
            SharedStateRegistry sharedStateRegistry,
            Executor executor) {
        super(sharedStateRegistry);
        this.fs = checkNotNull(fs, "FileSystem");
        checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
        this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
        this.readerWriterHelper = checkNotNull(readerWriterHelper, "FileSystemReaderWriterHelper");
        this.pathPrefix = checkNotNull(pathPrefix, "filesystem pathPrefix");
        this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
        this.completedCheckpoints.addAll(completedCheckpoints);
        this.executor = checkNotNull(executor, "Executor");
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return false;
    }

    /**
     * Synchronously writes the new checkpoints to FileSystem and asynchronously removes older ones.
     *
     * @param checkpoint Completed checkpoint to add.
     * @throws PossibleInconsistentStateException if adding the checkpoint failed and leaving the
     *     system in a possibly inconsistent state, i.e. it's uncertain whether the checkpoint
     *     metadata was fully written to the underlying systems or not.
     */
    @Override
    public CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
            final CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {
        checkNotNull(checkpoint, "CompletedCheckpoint");

        final Path path = getPathForCheckpoint(checkpoint.getCheckpointID());
        readerWriterHelper.write(path, checkpoint);
        completedCheckpoints.addLast(checkpoint);

        Optional<CompletedCheckpoint> subsume =
                CheckpointSubsumeHelper.subsume(
                        completedCheckpoints,
                        maxNumberOfCheckpointsToRetain,
                        completedCheckpoint ->
                                tryRemoveCompletedCheckpoint(
                                        completedCheckpoint,
                                        completedCheckpoint.shouldBeDiscardedOnSubsume(),
                                        checkpointsCleaner,
                                        postCleanup));
        unregisterUnusedState(completedCheckpoints);

        if (subsume.isPresent()) {
            LOG.debug("Added {} to {} without any older checkpoint to subsume.", checkpoint, path);
        } else {
            LOG.debug("Added {} to {} and subsume {}.", checkpoint, path, subsume);
        }
        return subsume.orElse(null);
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
        return new ArrayList<>(completedCheckpoints);
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return completedCheckpoints.size();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfCheckpointsToRetain;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        super.shutdown(jobStatus, checkpointsCleaner);
        if (jobStatus.isGloballyTerminalState()) {
            LOG.info("Shutting down FileSystemCompletedCheckpointStore");
            long lowestRetained = Long.MAX_VALUE;
            for (CompletedCheckpoint checkpoint : completedCheckpoints) {
                try {
                    if (!tryRemoveCompletedCheckpoint(
                            checkpoint,
                            checkpoint.shouldBeDiscardedOnShutdown(jobStatus),
                            checkpointsCleaner,
                            () -> {})) {
                        lowestRetained = Math.min(lowestRetained, checkpoint.getCheckpointID());
                    }
                } catch (Exception e) {
                    LOG.warn("Fail to remove checkpoint during shutdown.", e);
                    if (!checkpoint.shouldBeDiscardedOnShutdown(jobStatus)) {
                        lowestRetained = Math.min(lowestRetained, checkpoint.getCheckpointID());
                    }
                }
            }

            fs.delete(pathPrefix, true);
            completedCheckpoints.clear();

            // Now discard the shared state of not subsumed checkpoints - only if:
            // - the job is in a globally terminal state. Otherwise,
            // it can be a suspension, after which this state might still be needed.
            // - checkpoint is not retained (it might be used externally)
            // - checkpoint handle removal succeeded (e.g. from ZK) - otherwise, it might still
            // be used in recovery if the job status is lost
            getSharedStateRegistry().unregisterUnusedState(lowestRetained);
        } else {
            LOG.info("Suspending");
            // Clear the local handles, but don't remove any state
            completedCheckpoints.clear();
        }
    }

    // ------------------------------------------------------------------------

    private boolean tryRemoveCompletedCheckpoint(
            CompletedCheckpoint completedCheckpoint,
            boolean shouldDiscard,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup) {
        executor.execute(
                () -> {
                    try {
                        final Path path =
                                getPathForCheckpoint(completedCheckpoint.getCheckpointID());
                        fs.delete(path, false);
                        checkpointsCleaner.cleanCheckpoint(
                                completedCheckpoint, shouldDiscard, postCleanup, executor);
                    } catch (Exception e) {
                        LOG.warn(
                                "Could not discard completed checkpoint {}.",
                                completedCheckpoint.getCheckpointID(),
                                e);
                    }
                });

        return shouldDiscard;
    }

    private Path getPathForCheckpoint(long checkpointId) {
        return new Path(pathPrefix, String.format("/%019d", checkpointId));
    }
}
