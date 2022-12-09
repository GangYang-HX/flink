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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.throwable.UnrecoverableThrowableChecker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Abstract implementation of {@link CheckpointHandler} that does not maintain state
 * which is already maintained in {@link CheckpointCoordinator}. For methods need to
 * get and set state in {@link CheckpointCoordinator}, the corresponding methods will
 * just delegate to methods in {@link CheckpointCoordinator}.
 *
 * NOTE: We call methods of {@link CheckpointCoordinator} in this class instead of in
 * the implementations because the implementations may not be in the same package as
 * {@link CheckpointCoordinator}.
 */
public abstract class AbstractCheckpointHandler
        implements CheckpointHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointHandler.class);

    /** The job whose checkpoint this coordinator coordinates. */
    protected final JobID job;

    protected CheckpointCoordinator checkpointCoordinator;

    protected AbstractCheckpointHandler(
            CheckpointCoordinator checkpointCoordinator,
            JobID job) {
        this.job = job;
        this.checkpointCoordinator = checkpointCoordinator;
    }

    protected AbstractCheckpointHandler(JobID job) {
        this.job = job;
    }

    public void setCheckpointCoordinator(CheckpointCoordinator coordinator) {
        this.checkpointCoordinator = coordinator;
    }

    protected ExecutionVertex[] getTasksToCommitTo() {
        return checkpointCoordinator.getTasksToCommitTo();
    }

    protected PendingCheckpoint getPendingCheckpoint(long checkpointId) {
        return checkpointCoordinator.getPendingCheckpoints().get(checkpointId);
    }

    protected void logDeclinedCheckpoint(
            long checkpointId,
            DeclineCheckpoint message,
            String taskManagerLocationInfo,
            String reason) {
        checkpointCoordinator.logDeclinedCheckpoint(checkpointId, message, taskManagerLocationInfo, reason);
    }

    /**
     * Add this method (for its sub-class to call) so that
     * {@link CheckpointCoordinator#abortPendingCheckpoint(PendingCheckpoint, CheckpointException)}
     * doesn't have to be public.
     */
    protected void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            CheckpointException exception) {
        checkpointCoordinator.abortPendingCheckpoint(pendingCheckpoint, exception);
    }

    protected void abortPendingCheckpoint(
            PendingCheckpoint checkpoint,
            CheckpointException exception,
            ExecutionAttemptID taskExecutionId) {
        checkpointCoordinator.abortPendingCheckpoint(checkpoint, exception, taskExecutionId);
    }

    protected void sendAbortedMessages(long checkpointId, long timeStamp) {
        checkpointCoordinator.sendAbortedMessages(checkpointId, timeStamp);
    }

    protected void sendAbortedMessages(
            long checkpointId,
            long timestamp,
            Collection<ExecutionVertex> executionVertices) {
        checkpointCoordinator.sendAbortedMessages(checkpointId, timestamp, executionVertices);
    }

    protected void rememberRecentCheckpointId(long id) {
        checkpointCoordinator.rememberRecentCheckpointId(id);
    }

    protected Collection<OperatorCoordinatorCheckpointContext> getCoordinatorsToCheckpoint() {
        return checkpointCoordinator.coordinatorsToCheckpoint;
    }

    protected CompletedCheckpoint completePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        return checkpointCoordinator.completePendingCheckpoint(pendingCheckpoint);
    }

    protected Map<Long, PendingCheckpoint> getAllPendingCheckpoints() {
        return checkpointCoordinator.getPendingCheckpoints();
    }

    protected void abortPendingCheckpoints(
            Predicate<PendingCheckpoint> checkpointToFailPredicate,
            CheckpointException exception) {
        checkpointCoordinator.abortPendingCheckpoints(checkpointToFailPredicate, exception);
    }

    protected boolean isPendingCheckpoint(long checkpointId) {
        return checkpointCoordinator.getPendingCheckpoints().containsKey(checkpointId);
    }

    protected void acknowledgeTask(
            long checkpointId,
            AcknowledgeCheckpoint message,
            String taskManagerLocationInfo,
            PendingCheckpoint checkpoint)
            throws CheckpointException{

        switch (checkpoint.acknowledgeTask(
                message.getTaskExecutionId(),
                message.getSubtaskState(),
                message.getCheckpointMetrics())) {
            case SUCCESS:
                LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                        checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

                if (checkpoint.areTasksFullyAcknowledged()) {
                    completePendingCheckpoint(checkpoint);
                }
                break;

            case DUPLICATE:
                LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                        message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
                break;

            case UNKNOWN:
                LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
                                "because the task's execution attempt id was unknown. Discarding " +
                                "the state handle to avoid lingering state.", message.getCheckpointId(),
                        message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

                discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

                break;

            case DISCARDED:
                LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
                                "because the pending checkpoint had been discarded. Discarding the " +
                                "state handle tp avoid lingering state.",
                        message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());
        }
    }

    protected void discardSubtaskState(
            final JobID jobId,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final TaskStateSnapshot subtaskState) {
        checkpointCoordinator.discardSubtaskState(jobId, executionAttemptID, checkpointId, subtaskState);
    }

    @Override
    public void processDeclineReason(CheckpointFailureManager failureManager, Throwable reason, long checkpointId) {
        if (UnrecoverableThrowableChecker.check(reason)) {
            failureManager.handleJobLevelUnRecoverableDeclineException(reason, checkpointId);
        }
    }
}
