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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * An implementation of {@link CheckpointHandler} whose behaviors are extracted from {@link CheckpointCoordinator}.
 */
public class GlobalCheckpointHandler extends AbstractCheckpointHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalCheckpointHandler.class);

    protected GlobalCheckpointHandler(
            CheckpointCoordinator coordinator,
            JobID job) {
        super(coordinator, job);
    }

    @Override
    public boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message,
            String taskManagerLocationInfo,
            PendingCheckpoint checkpoint)
            throws CheckpointException {
        long checkpointId = message.getCheckpointId();
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
                LOG.debug(
                        "Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getJob(),
                        taskManagerLocationInfo);
                break;

            case UNKNOWN:
                LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
                                "because the task's execution attempt id was unknown. Discarding " +
                                "the state handle to avoid lingering state.", message.getCheckpointId(),
                        message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

                checkpointCoordinator.discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());
                break;

            case DISCARDED:
                LOG.warn(
                        "Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
                                "because the pending checkpoint had been discarded. Discarding the " +
                                "state handle tp avoid lingering state.",
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getJob(),
                        taskManagerLocationInfo);

                checkpointCoordinator.discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());
        }

        return true;
    }

    @Override
    public void receiveDeclineMessage(
            DeclineCheckpoint message,
            String taskManagerLocationInfo,
            @Nullable PendingCheckpoint checkpoint)
            throws CheckpointException {

        long checkpointId = message.getCheckpointId();

        if (checkpoint != null) {
            Preconditions.checkState(
                    !checkpoint.isDiscarded(),
                    "Received message for discarded but non-removed checkpoint " + checkpointId);
            LOG.info("Decline checkpoint {} by task {} of job {} at {}, because of {}.",
                    checkpointId,
                    message.getTaskExecutionId(),
                    job,
                    taskManagerLocationInfo,
					ExceptionUtils.stringifyException(message.getReason()));
            final CheckpointException checkpointException;
            if (message.getReason() == null) {
                checkpointException =
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED);
            } else {
                checkpointException = CheckpointCoordinator.getCheckpointException(
                        CheckpointFailureReason.JOB_FAILURE, message.getReason());
            }
            abortPendingCheckpoint(
                    checkpoint,
                    checkpointException,
                    message.getTaskExecutionId());
        } else if (LOG.isDebugEnabled()) {
            final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");
            logDeclinedCheckpoint(checkpointId, message, taskManagerLocationInfo, reason);
        }
    }

    @Override
    public void processFailedTask(
            ExecutionAttemptID executionAttemptId,
            Throwable cause) {
        abortPendingCheckpoints(
                checkpoint -> !checkpoint.isAcknowledgedBy(executionAttemptId),
                new CheckpointException(CheckpointFailureReason.TASK_FAILURE, cause));
    }

    @Override
    public void expirePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        abortPendingCheckpoint(
                pendingCheckpoint,
                pendingCheckpoint.getExpiredCheckpointAnalyzer().getExpiredExceptionWithDetail());
    }

    @Override
    public void sendAcknowledgeMessages(long checkpointId, long timestamp) {
        // commit tasks
        for (ExecutionVertex ev : checkpointCoordinator.getTasksToCommitTo()) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointComplete(checkpointId, timestamp);
            }
        }

        // commit coordinators
        for(OperatorCoordinatorCheckpointContext coordinatorContext : checkpointCoordinator.getCoordinatorsToCheckpoint()) {
            coordinatorContext.checkpointComplete(checkpointId);
        }
    }
}
