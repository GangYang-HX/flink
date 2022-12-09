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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointHandler;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointHandler;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.throwable.UnrecoverableThrowableChecker;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * An implementation of {@link CheckpointHandler} based on {@link CheckpointRegion}. It treats a
 * {@link CheckpointRegion} as a unit. It tolerates a {@link CheckpointRegion}'s checkpoint failure or expiration
 * by logically mapping the state of a previously completed checkpoint to the failed checkpoint.
 */
public class RegionalCheckpointHandler extends AbstractCheckpointHandler {

    public static final Logger LOG = LoggerFactory.getLogger(RegionalCheckpointHandler.class);

    private static final long ZERO_VALUE_FOR_CHECKPOINT_ID = -1;

    private int maxTolerableConsecutiveFailuresOrExpirations;

    private double maxTolerableFailureOrExpirationRatio;

    private CheckpointTopology checkpointTopology;

    private Map<Long, CompletedCheckpoint> completedCheckpoints;

    // value: last completed checkpoint id and the id of the checkpoint it referenced,
    //        which may be the same as the last checkpoint when there is no loggical mapping.
    private Map<CheckpointRegion, Tuple2<Long, Long>> lastMappingInfoPerRegion;

    /**
     * Responsible for cleaning up {@link RegionalCheckpointHandler#completedCheckpoints}
     */
    private CompletedCheckpointsCleaner cleaner;

    /**
     * Save failed regions while calling
     * {@link RegionalCheckpointHandler#processFailedTask(ExecutionAttemptID, Throwable)}
     * and traverse it if all tasks acked.
     *
     * Since this method will only handle the largest pending checkpoint,
     * we don't need to use checkpoint id as a key.
     *
     * NOTE: we must clear it at last.
     */
    private Map<CheckpointRegion, Long> mappedCheckpointIdForFailedRegion;

    /**
     * How many times a {@link CheckpointRegion} has consecutively failed or expired. If a {@link CheckpointRegion}
     * failed more than the configured max tolerable consecutive failure or expire number, we will abort the checkpoint.
     */
    private Map<CheckpointRegion, Integer> failedOrExpiredTimes;

    /**
     * Keep track of the number of failed regions per checkpoint. If the failure or expiration ratio exceeds
     * the tolerable ratio, we will abort the checkpoint.
     */
    private Map<Long, Integer> failedOrExpiredRegionsPerCheckpoint;

    /**
     * value: f0 - checkpoint ID, f1 - status (succeeded (true) or failed (false)) of the checkpoint.
     */
    private Map<CheckpointRegion, List<Tuple2<Long, Boolean>>> checkpointStatusesPerRegion;

    /**
     * Failed or expired execution vertices for each checkpoint. We will not send acknowledgement to
     * these tasks when we finish the corresponding checkpoint.
     */
    private Map<Long, Set<ExecutionVertex>> failedOrExpiredVertices;

    /* Controls the count of completed checkpoint in memory */
    private static final int CLEAR_COMPLETED_CHECKPOINT_THRESHOLD = 32;

    private Runnable failFastProcessor = null;

    public RegionalCheckpointHandler(
            CheckpointCoordinator checkpointCoordinator,
            JobID job,
            CheckpointTopology topology,
            int maxTolerableConsecutiveFailuresOrExpirations,
            double maxTolerableFailureOrExpirationRatio) {
        this(
                checkpointCoordinator,
                job,
                topology,
                maxTolerableConsecutiveFailuresOrExpirations,
                maxTolerableFailureOrExpirationRatio,
                null);
    }

    public RegionalCheckpointHandler(
            JobID job,
            CheckpointTopology topology,
            int maxTolerableConsecutiveFailuresOrExpirations,
            double maxTolerableFailureOrExpirationRatio) {
        super(job);

        initialize(
                topology,
                maxTolerableConsecutiveFailuresOrExpirations,
                maxTolerableFailureOrExpirationRatio,
                null);
    }

    public RegionalCheckpointHandler(
            CheckpointCoordinator checkpointCoordinator,
            JobID job,
            CheckpointTopology topology,
            int maxTolerableConsecutiveFailuresOrExpirations,
            double maxTolerableFailureOrExpirationRatio,
            @Nullable CompletedCheckpoint restoredCheckpoint) {

        super(checkpointCoordinator, job);

        initialize(
                topology,
                maxTolerableConsecutiveFailuresOrExpirations,
                maxTolerableFailureOrExpirationRatio,
                restoredCheckpoint);
    }

    private void initialize(
            CheckpointTopology topology,
            int maxTolerableConsecutiveFailuresOrExpirations,
            double maxTolerableFailureOrExpirationRatio,
            @Nullable CompletedCheckpoint restoredCheckpoint) {

        this.checkpointTopology = topology;
        this.maxTolerableConsecutiveFailuresOrExpirations = maxTolerableConsecutiveFailuresOrExpirations;
        this.maxTolerableFailureOrExpirationRatio = maxTolerableFailureOrExpirationRatio;

        // NOTE: This should be called before calling restoreCheckpoint.
        this.lastMappingInfoPerRegion = new HashMap<>();

        this.completedCheckpoints = new HashMap<>();

        this.failedOrExpiredTimes = new HashMap<>();

        this.failedOrExpiredRegionsPerCheckpoint = new HashMap<>();

        this.checkpointStatusesPerRegion = new HashMap<>();

        this.failedOrExpiredVertices = new HashMap<>();

        this.mappedCheckpointIdForFailedRegion = new HashMap<>();

        this.cleaner = new CompletedCheckpointsCleaner();

        restoreCheckpoint(restoredCheckpoint);
    }

    public void restoreCheckpoint(CompletedCheckpoint restoredCheckpoint) {
        long checkpointId;
        if (restoredCheckpoint != null) {
            checkpointId = restoredCheckpoint.getCheckpointID();
        } else {
            checkpointId = ZERO_VALUE_FOR_CHECKPOINT_ID;
        }

        for (CheckpointRegion checkpointRegion : checkpointTopology.getCheckpointRegions()) {
            lastMappingInfoPerRegion.put(checkpointRegion, Tuple2.of(checkpointId, checkpointId));
        }
    }

    @Override
    public void sendAcknowledgeMessages(long checkpointId, long timeStamp) {
        // Acknowledge tasks that did not fail or expire.
        Set<ExecutionVertex> failedOrExpiredVertices = this.failedOrExpiredVertices
                .computeIfAbsent(checkpointId, ci -> new HashSet<>());
        Arrays.stream(getTasksToCommitTo())
                .filter(t -> !failedOrExpiredVertices.contains(t))
                .map(ExecutionVertex::getCurrentExecutionAttempt)
                .filter(Objects::nonNull)
                .forEach(ee -> ee.notifyCheckpointComplete(checkpointId, timeStamp));

        // Abort tasks that failed or expired so that the StateBackend can do necessary cleanup.
        failedOrExpiredVertices
                .stream()
                .map(ExecutionVertex::getCurrentExecutionAttempt)
                .forEach(ee -> ee.notifyCheckpointAborted(checkpointId, timeStamp));

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : getCoordinatorsToCheckpoint()) {
            OperatorID operatorId = coordinatorContext.operatorId();
            int subtaskIndex = coordinatorContext.currentParallelism();
            OperatorInstanceID operatorInstanceId = new OperatorInstanceID(subtaskIndex, operatorId);
            if (!isOperatorFailedOrExpired(checkpointId, operatorInstanceId)) {
                coordinatorContext.checkpointComplete(checkpointId);
            }
        }
    }

    @Override
    public void receiveDeclineMessage(
            DeclineCheckpoint message,
            String taskManagerLocationInfo,
            @Nullable PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {

        long checkpointId = message.getCheckpointId();
        ExecutionAttemptID executionAttempt = message.getTaskExecutionId();
        CheckpointRegion region = checkpointTopology.getCheckpointRegion(executionAttempt);

        if (checkFailureOrExpirationThreshold(checkpointId, region, executionAttempt)) {
            return;
        }

        pendingCheckpoint = getPendingCheckpoint(checkpointId);

        if (pendingCheckpoint != null) {
            Preconditions.checkState(
                    !pendingCheckpoint.isDiscarded(),
                    "Received message for discarded but non-removed checkpoint " + checkpointId);
            LOG.info("Decline checkpoint {} by task {} of job {} at {}, because of {}. " +
                            "Logically map previous checkpoint's state to it.",
                    checkpointId,
                    message.getTaskExecutionId(),
                    job,
                    taskManagerLocationInfo,
					ExceptionUtils.stringifyException(message.getReason()));

            if (checkpointStatusesPerRegion.containsKey(region)) {
                if (isRegionFailedOrExpiredForCheckpoint(region, checkpointId)) {
                    return;
                } else if (isRegionSucceededForCheckpoint(region, checkpointId)) {
                    throw new IllegalStateException(
                            "A succeeded region contains declined checkpoint. This is a bug.");
                }
            }
            List<Tuple2<Long, Boolean>> statuses = checkpointStatusesPerRegion.computeIfAbsent(region, r -> new ArrayList<>());
            statuses.add(new Tuple2<>(checkpointId, false));

            // There is no succeeded checkpoint for this region.
            if (!lastMappingInfoPerRegion.containsKey(region)
                    || lastMappingInfoPerRegion.get(region).f1 == ZERO_VALUE_FOR_CHECKPOINT_ID) {
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        new CheckpointException(
                                "There is no succeeded checkpoint for region " + region,
                                CheckpointFailureReason.CHECKPOINT_DECLINED),
                        executionAttempt);
                return;
            }

            // Logically map states of operators belonging to this region to previously succeeded checkpoint.
            CompletedCheckpoint completedCheckpoint = findCompletedCheckpoint(region);
            if (completedCheckpoint == null) {
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        new CheckpointException(CheckpointFailureReason.NO_SUCCEEDED_CHECKPOINT_FOR_REGION),
                        executionAttempt);
            } else {
                switch (logicallyMapState(region, completedCheckpoint, pendingCheckpoint)) {
                    case SUCCESS:
                        LOG.info("Successfully mapped (logically) checkpoint region {} " +
                                        "from completed checkpoint {} to pending checkpoint {} of job {}. " +
                                        "This mapping happens because checkpoint for {} was declined.",
                                region,
                                completedCheckpoint.getCheckpointID(),
                                checkpointId,
                                message.getJob(),
                                executionAttempt);

                        completePendingCheckpointIfNecessary(checkpointId, pendingCheckpoint);
                        break;

                    case UNKNOWN:
                        throw new IllegalStateException("Unknown execution vertex. This should not happen.");
                }
            }
        } else {
            final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");
            logDeclinedCheckpoint(checkpointId, message, taskManagerLocationInfo, reason);
        }
    }

    @Override
    public void processFailedTask(ExecutionAttemptID executionAttemptId, Throwable reason)
            throws CheckpointException {
        // For now, we just keep the latest pending checkpoint (and discard all the other pending checkpoints),
        // and map state from previous completed checkpoint to it.
        // Maybe we should keep all the pending checkpoints, and do the state mapping for all of them.

        CheckpointRegion region = checkpointTopology.getCheckpointRegion(executionAttemptId);
        if (region == null) {
            LOG.warn("Cannot find CheckpointRegion for {}", executionAttemptId);
            return;
        }

        CheckpointException checkpointException = new CheckpointException(
                "Discarding pending checkpoints whose checkpoint ID is less than the max checkpoint ID",
                CheckpointFailureReason.TASK_FAILURE);

        // Logically map `region` in all pending checkpoints to the last completed checkpoint.
        long maxCheckpointId = -1;
        for (Iterator<Map.Entry<Long, PendingCheckpoint>> entryIter =
             getAllPendingCheckpoints().entrySet().iterator();
             entryIter.hasNext();) {
            Map.Entry<Long, PendingCheckpoint> entry = entryIter.next();
            long checkpointId = entry.getKey();
            PendingCheckpoint pendingCheckpoint = entry.getValue();

            if (pendingCheckpoint.isDiscarded()) {
                continue;
            }

            if (checkpointId < maxCheckpointId) {
                // Abort checkpoint whose ID is less than the `maxCheckpointId`
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        checkpointException);
                entryIter.remove();
                continue;
            }

            maxCheckpointId = checkpointId;
        }

        for (Iterator<Map.Entry<Long, PendingCheckpoint>> entryIter =
             getAllPendingCheckpoints().entrySet().iterator();
             entryIter.hasNext();) {

            Map.Entry<Long, PendingCheckpoint> entry = entryIter.next();
            long checkpointId = entry.getKey();
            PendingCheckpoint pendingCheckpoint = entry.getValue();

            if (pendingCheckpoint.isDiscarded()) {
                continue;
            }

            if (checkpointId != maxCheckpointId) {
                abortPendingCheckpoint(pendingCheckpoint, checkpointException);
                entryIter.remove();
                continue;
            }

            LOG.info("Task {} of job {} failed. Logically map previous checkpoint's state to checkpoint {}.",
                    executionAttemptId,
                    job,
                    checkpointId);

            if (checkpointStatusesPerRegion.containsKey(region)) {
                // For the following two situations:
                // 1. if isRegionFailedOrExpiredForCheckpoint(region, checkpointId) is true
                //    The region has been mapped before (i.e. because another task in the region has failed).
                // 2. if isRegionFailedOrExpiredForCheckpoint(region, checkpointId) is false
                //    The region has completed in checkpoint `checkpointId`. Keep it as it is.
                // We have nothing to do for this pending checkpoint.
                break;
            }

            // There is no succeeded checkpoint for this region.
            if (!lastMappingInfoPerRegion.containsKey(region)
                    || lastMappingInfoPerRegion.get(region).f1 == ZERO_VALUE_FOR_CHECKPOINT_ID) {
                abortPendingCheckpoints(
                        ignored -> true,
                        new CheckpointException(
                                String.format("There is no succeeded checkpoint for region %s", region),
                                CheckpointFailureReason.CHECKPOINT_DECLINED));
                return;
            }

            // Logically map states of operators belonging to this region to previously succeeded checkpoint.
            CompletedCheckpoint completedCheckpoint = findCompletedCheckpoint(region);
            if (completedCheckpoint == null) {
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        new CheckpointException(CheckpointFailureReason.NO_SUCCEEDED_CHECKPOINT_FOR_REGION),
                        executionAttemptId);
            } else {
                switch (logicallyMapState(region, completedCheckpoint, pendingCheckpoint)) {
                    case SUCCESS:
                        LOG.info("Successfully mapped (logically) checkpoint region {} " +
                                        "from completed checkpoint {} to pending checkpoint {}. " +
                                        "This mapping happens because task {} failed.",
                                region, completedCheckpoint.getCheckpointID(), checkpointId, executionAttemptId);
                        mappedCheckpointIdForFailedRegion.put(region, completedCheckpoint.getCheckpointID());
                        if (pendingCheckpoint.areTasksFullyAcknowledged()) {
                            CompletedCheckpoint justCompletedCheckpoint = completePendingCheckpoint(pendingCheckpoint);
                            checkpointTopology.getCheckpointRegions()
                                  .forEach(cr -> {
                                      if (mappedCheckpointIdForFailedRegion.containsKey(cr)) {
                                          lastMappingInfoPerRegion.put(cr, Tuple2.of(
                                                  checkpointId,
                                                  mappedCheckpointIdForFailedRegion.get(cr)));
                                          addRegionStatus(region, checkpointId, false);
                                      } else {
                                          lastMappingInfoPerRegion.put(cr, Tuple2.of(
                                                  checkpointId,
                                                  checkpointId));
                                          addRegionStatus(region, checkpointId, true);
                                      }
                                  });
                            completedCheckpoints.put(checkpointId, justCompletedCheckpoint);
                            mappedCheckpointIdForFailedRegion.clear();
                        }
                        break;
                    case UNKNOWN:
                        throw new IllegalStateException("Unknown execution vertex. This should not happen.");
                }
            }
        }
    }

    @Override
    public void expirePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        long checkpointId = pendingCheckpoint.getCheckpointId();
        Map<CheckpointRegion, Long> notCompletedCheckpointRegions = new HashMap<>();
        boolean exceededTolerance;
        Set<CheckpointRegion> expiredRegions = checkpointTopology.getCheckpointRegions()
                .stream()
                .filter(cr -> !isCheckpointRegionSucceeded(pendingCheckpoint, cr)
                            && !isRegionFailedOrExpiredForCheckpoint(cr, checkpointId))
                .collect(Collectors.toSet());
        // Check if the maximum tolerance limit was exceeded.
        exceededTolerance = expiredRegions.stream().anyMatch(cr ->
                checkFailureOrExpirationThreshold(checkpointId, cr, null));
        if (exceededTolerance) {
            return;
        }
        expiredRegions.forEach(cr -> {
            CompletedCheckpoint completedCheckpoint = findCompletedCheckpoint(cr);
            if (completedCheckpoint == null) {
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        new CheckpointException(CheckpointFailureReason.NO_SUCCEEDED_CHECKPOINT_FOR_REGION));
            } else {
                switch (logicallyMapState(cr, completedCheckpoint, pendingCheckpoint)) {
                    case SUCCESS:
                        // record region and mapping id
                        notCompletedCheckpointRegions.put(cr, completedCheckpoint.getCheckpointID());
                        LOG.info(
                                "Successfully mapped (logically) checkpoint region {} " +
                                        "from completed checkpoint {} to pending checkpoint {}. " +
                                        "This mapping happens because checkpoint {} expired before complete.",
                                cr,
                                completedCheckpoint.getCheckpointID(),
                                checkpointId,
                                checkpointId);
                        break;
                    case UNKNOWN:
                        throw new IllegalStateException("Unknown execution vertex. This should not happen.");
                }
            }
        });
        CompletedCheckpoint justCompletedCheckpoint = completePendingCheckpoint(pendingCheckpoint);
        // Clear last mapping checkpoint reference.
        // eg: ck-1 complete, ck-2 expired, ck-3 should clear reference about ck-2
        lastMappingInfoPerRegion.forEach((cr, mappingInfo) -> {
            long lastCheckpointId = mappingInfo.f0;
            long lastMappingId = mappingInfo.f1;

            // We can do some clearing when there is logically mapping
            handleReference(checkpointId, lastCheckpointId, lastMappingId);

        });
        for (CheckpointRegion cr : checkpointTopology.getCheckpointRegions()) {
            if (!notCompletedCheckpointRegions.containsKey(cr)) {
                lastMappingInfoPerRegion.put(cr, Tuple2.of(
                        checkpointId,
                        Math.max(lastMappingInfoPerRegion.get(cr).f1, checkpointId)));
            } else {
                lastMappingInfoPerRegion.put(cr, Tuple2.of(checkpointId, notCompletedCheckpointRegions.get(cr)));
            }
        }
        cleaner.cleanupCompletedCheckpointsIfNecessary();
        completedCheckpoints.put(checkpointId, justCompletedCheckpoint);
    }

    private void handleReference(
            long currentCheckpointId,
            long lastCheckpointId,
            long lastMappingId) {
        if (isLogicallyMapped(lastCheckpointId, lastMappingId)) {
            // In order to resolve this issue: https://git.bilibili.co/datacenter/flink/-/issues/202,
            // we should avoid the problems caused by out-of-order.

            // All checkpoints were completed in order.
            if (checkpointCoordinator.isSmallestPendingCheckpointId(currentCheckpointId)) {
                CompletedCheckpoint lastMappingCheckpoint =
                        completedCheckpoints.get(lastMappingId);
                checkpointCoordinator.getCheckpointStore()
                        .dereference(lastCheckpointId, lastMappingCheckpoint);
                if (!lastMappingCheckpoint.isReferenced()) {
                    // If there are no more references, we can safely delete it.
                    completedCheckpoints.remove(lastMappingId);
                    lastMappingCheckpoint.doDiscard();
                    LOG.info("current checkpoint {}, removed no referenced checkpoint {}",
                             currentCheckpointId, lastMappingId);
                }
            }
            // Some checkpoints were completed out of order(checkpoints with larger IDs were completed
            // before checkpoints with smaller IDS).
            else {
                // NOTE: We need to clear immediately, otherwise it may cause the reference to never be
                // deleted. For example, ck-17 references ck-1, but since ck-17 itself is not referenced,
                // during cleanup while storing a new complete checkpoint, ck-17 will be removed,
                // and ck-1 cannot be deleted forever.
                cleaner.notifyClear();
            }
        }
    }


    /**
     * Whether there is a logical mapping for region state.
     */
    private boolean isLogicallyMapped(long lastCheckpointId, long lastMappingId) {
        return lastMappingId != lastCheckpointId && lastMappingId != ZERO_VALUE_FOR_CHECKPOINT_ID;
    }

    private void doCleanup() {
        Set<Long> checkpointsToBeDiscarded = new HashSet<>();
        Iterator<Long> ids = new TreeSet<>(completedCheckpoints.keySet()).descendingIterator();

        // The checkpoint id of the latest checkpoint being referenced.
        Set<Long> checkpointsReferencedByLastCheckpoint = lastMappingInfoPerRegion.values()
                .stream()
                .map(t -> t.f1)
                .collect(Collectors.toSet());
        while (ids.hasNext()) {
            long id = ids.next();
            CompletedCheckpoint oldCompletedCheckpoint = completedCheckpoints.get(id);
            if (!oldCompletedCheckpoint.isReferenced()) {
                checkpointsToBeDiscarded.add(id);
            } else {
                Set<Long> referencedCheckpoints = oldCompletedCheckpoint.getReferencingCheckpointIds()
                        .stream()
                        .filter(checkpointsToBeDiscarded::contains)
                        .collect(Collectors.toSet());
                if (referencedCheckpoints.size() > 0) {
                    if (checkpointsReferencedByLastCheckpoint.contains(id)) {
                        // We cannot discard it, because of it's referenced by the latest checkpoint,
                        // but the number of corresponding references needs to be reduced.
                        referencedCheckpoints.forEach(referencedId -> checkpointCoordinator
                                .getCheckpointStore().dereference(referencedId, oldCompletedCheckpoint));
                    } else {
                        // It can be discarded directly.
                        checkpointsToBeDiscarded.add(id);
                    }
                }
            }
        }
        // Remove to be discarded checkpoint.
        checkpointsToBeDiscarded.forEach(id -> {
            completedCheckpoints.get(id).doDiscard();
            completedCheckpoints.remove(id);
        });
        LOG.info("clear and discard checkpoint ids: {}", checkpointsToBeDiscarded);
        checkpointsToBeDiscarded.clear();
    }

    @Override
    public boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message,
            String taskManagerLocationInfo,
            PendingCheckpoint checkpoint)
            throws CheckpointException {

        long checkpointId = message.getCheckpointId();
        ExecutionAttemptID executionId = message.getTaskExecutionId();
        CheckpointRegion region = checkpointTopology.getCheckpointRegion(executionId);
        if (region == null) {
            LOG.warn("Cannot find CheckpointRegion for {}", executionId);
            return true;
        }

        if (checkpointStatusesPerRegion.containsKey(region)) {
            if (isRegionSucceededForCheckpoint(region, checkpointId)) {
                throw new IllegalStateException(
                        "Acknowledge checkpoint message received for an already completed checkpoint region");
            } else if (isRegionFailedOrExpiredForCheckpoint(region, checkpointId)) {
                // Some tasks in the checkpoint region have already failed.
                return true;
            }
        }

        switch (checkpoint.acknowledgeTask(
                message.getTaskExecutionId(),
                message.getSubtaskState(),
                message.getCheckpointMetrics())) {
            case SUCCESS:
                LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                        checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

                if (isCheckpointRegionSucceeded(checkpoint, region)) {
                    // Clear consecutive failures/expires of this region.
                    failedOrExpiredTimes.put(region, 0);
                }

                completePendingCheckpointIfNecessary(checkpointId, checkpoint);
                break;

            case DUPLICATE:
                LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
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

                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());

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

        return true;
    }

    @Override
    public void processDeclineReason(CheckpointFailureManager failureManager, Throwable reason, long checkpointId) {
        if (UnrecoverableThrowableChecker.check(reason)) {
            // Fail job after tasks mapping state, For regional checkpoint,
            // the latest pending checkpoint can be guaranteed to be completed.
            failFastProcessor = () -> failureManager.handleJobLevelUnRecoverableDeclineException(reason, checkpointId);
        }
    }

    private void completePendingCheckpointIfNecessary(
            long currentCheckpointId,
            PendingCheckpoint checkpoint)
            throws CheckpointException {
        if (checkpoint.areTasksFullyAcknowledged()) {
            // Handle reference count, and clear unuseful completed checkpoint.
            checkpointTopology.getCheckpointRegions()
                  .stream()
                  .filter(cr -> !isRegionFailedOrExpiredForCheckpoint(cr, currentCheckpointId))
                  .forEach(r -> {
                      Tuple2<Long, Long> mappingInfo = lastMappingInfoPerRegion.get(r);
                      long lastCheckpointId = mappingInfo.f0;
                      long lastMappingId = mappingInfo.f1;
                      // We can do some clearing when there is logically mapping
                      handleReference(currentCheckpointId, lastCheckpointId, lastMappingId);
                      lastMappingInfoPerRegion.put(r, Tuple2.of(
                              currentCheckpointId,
                              Math.max(lastMappingId, currentCheckpointId)));
                  });

            cleaner.cleanupCompletedCheckpointsIfNecessary();
            completedCheckpoints.put(currentCheckpointId, completePendingCheckpoint(checkpoint));

            List<ExecutionVertex> executionVerticesToAbort =
                    checkpointTopology.getCheckpointRegions().stream()
                                      .filter(cr -> isRegionFailedOrExpiredForCheckpoint(cr, currentCheckpointId))
                                      .flatMap( cr -> cr.getAllExecutionAttempts()
                                                        .stream()
                                                        .map(cr::getExecutionVertex))
                                      .collect(Collectors.toList());
            sendAbortedMessages(currentCheckpointId, System.currentTimeMillis(), executionVerticesToAbort);
            if (failFastProcessor != null) {
                failFastProcessor.run();
            }
        }
    }

    private boolean checkFailureOrExpirationThreshold(
            long checkpointId,
            CheckpointRegion region,
            ExecutionAttemptID executionAttempt) {
        int times = failedOrExpiredTimes.getOrDefault(region, 0) + 1;
        if (times > maxTolerableConsecutiveFailuresOrExpirations) {
            PendingCheckpoint pendingCheckpoint = getPendingCheckpoint(checkpointId);
            if (pendingCheckpoint != null && !pendingCheckpoint.isDiscarded()) {
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        new CheckpointException(
                                CheckpointFailureReason.MAX_CONSECUTIVE_FAILURES_OR_EXPIRES_OF_CHECKPOINT_REGION_EXCEEDED),
                        executionAttempt);
            }
            return true;
        }
        failedOrExpiredTimes.put(region, times);

        int numOfFailedOrExpiredRegions = failedOrExpiredRegionsPerCheckpoint.getOrDefault(checkpointId, 0) + 1;
        double ratio = (double) numOfFailedOrExpiredRegions / checkpointTopology.getCheckpointRegions().size();
        if (ratio > maxTolerableFailureOrExpirationRatio) {
            PendingCheckpoint pendingCheckpoint = getPendingCheckpoint(checkpointId);
            if (pendingCheckpoint != null && !pendingCheckpoint.isDiscarded()) {
                abortPendingCheckpoint(
                        pendingCheckpoint,
                        new CheckpointException(
                                CheckpointFailureReason.MAX_FAILED_OR_EXPIRED_RATIO_OF_CHECKPOINT_REGIONS_EXCEEDED),
                        executionAttempt);
            }
            return true;
        }
        failedOrExpiredRegionsPerCheckpoint.put(checkpointId, numOfFailedOrExpiredRegions);

        return false;
    }

    /**
     * Logically map operator subtask state from the given {@link CompletedCheckpoint}
     * to the given {@link PendingCheckpoint}. We also update necessary fields here.
     */
    private PendingCheckpoint.TaskAcknowledgeResult logicallyMapState(
            CheckpointRegion region,
            CompletedCheckpoint completedCheckpoint,
            PendingCheckpoint pendingCheckpoint) {
        // Mark referencing for the subtask state of all the operators that belong to the given region.
        completedCheckpoint.reference(pendingCheckpoint.getCheckpointId());

        // Mark all execution vertices belong to this region as failed or expired.
        region.getAllExecutionAttempts().forEach(eai -> {
            ExecutionVertex executionVertex = region.getExecutionVertex(eai);
            Set<ExecutionVertex> failedOrExpiredVertices = this.failedOrExpiredVertices.computeIfAbsent(
                    pendingCheckpoint.getCheckpointId(),
                    ci -> new HashSet<>());
            failedOrExpiredVertices.add(executionVertex);
        });

        return pendingCheckpoint.acknowledgeCheckpointRegion(region, completedCheckpoint);
    }

    private boolean isRegionSucceededForCheckpoint(CheckpointRegion region, long checkpointId) {
        if (!checkpointStatusesPerRegion.containsKey(region)) {
            return false;
        }

        List<Tuple2<Long, Boolean>> statuses = checkpointStatusesPerRegion.get(region);
        for (Tuple2<Long, Boolean> status : statuses) {
            if (status.f0 == checkpointId && status.f1) {
                return true;
            }
        }

        return false;
    }

    private boolean isRegionFailedOrExpiredForCheckpoint(CheckpointRegion region, long checkpointId) {
        if (!checkpointStatusesPerRegion.containsKey(region)) {
            return false;
        }

        List<Tuple2<Long, Boolean>> statuses = checkpointStatusesPerRegion.get(region);
        for (Tuple2<Long, Boolean> status : statuses) {
            if (status.f0 == checkpointId && !status.f1) {
                return true;
            }
        }

        return false;
    }

    private boolean isOperatorFailedOrExpired(long checkpointId, OperatorInstanceID operatorInstanceId) {
        long startTime = System.nanoTime();
        boolean failedOrExpired = failedOrExpiredVertices.getOrDefault(checkpointId, new HashSet<>())
                .stream()
                .flatMap(ev -> {
                    int subtaskIndex = ev.getParallelSubtaskIndex();
                    return ev.getJobVertex().getOperatorIDs()
                            .stream()
                            .map(oip -> new OperatorInstanceID(subtaskIndex, oip.getGeneratedOperatorID()));
                })
                .collect(Collectors.toSet())
                .contains(operatorInstanceId);
        LOG.info("It takes {} nano-seconds to check whether {} failed or expired",
                System.nanoTime() - startTime,
                operatorInstanceId);

        return failedOrExpired;
    }

    @Nullable
    private CompletedCheckpoint findCompletedCheckpoint(CheckpointRegion region) {
        Tuple2<Long, Long> mappingInfo = lastMappingInfoPerRegion.get(region);
        if (mappingInfo.f0 == ZERO_VALUE_FOR_CHECKPOINT_ID || mappingInfo.f1 == ZERO_VALUE_FOR_CHECKPOINT_ID) {
            return null;
        } else {
            return completedCheckpoints.get(mappingInfo.f1);
        }
    }

    private void addRegionStatus(CheckpointRegion region, long checkpointId, boolean status) {
        List<Tuple2<Long, Boolean>> statuses = checkpointStatusesPerRegion.computeIfAbsent(
                region, r -> new ArrayList<>());
        statuses.add(new Tuple2<>(checkpointId, status));
        if (status) {
            // Cleanup older completed checkpoint.
            statuses.removeIf(checkpointIdAndStatus ->
                    !isPendingCheckpoint(checkpointIdAndStatus.f0) && checkpointIdAndStatus.f0 <= checkpointId);
        }
    }

    /**
     * Check whether all the execution attempts of the given checkpoint region are all
     * acknowledged by the corresponding task.
     */
    private boolean isCheckpointRegionSucceeded(PendingCheckpoint checkpoint, CheckpointRegion region) {
        return !isRegionFailedOrExpiredForCheckpoint(region, checkpoint.getCheckpointId()) &&
                region.getAllExecutionAttempts()
                        .stream()
                        .allMatch(checkpoint::isAcknowledgedBy);
    }

    private class CompletedCheckpointsCleaner {

        boolean shouldClear;

        CompletedCheckpointsCleaner() {
            this.shouldClear = false;
        }

        void notifyClear() {
            shouldClear = true;
        }

        /**
         * Clear useless completed checkpoints.
         * NOTE: This method call depends on the latest {@link RegionalCheckpointHandler#lastMappingInfoPerRegion}.
         *       That is to say, before cleaning, you need to ensure that lastMappingInfoPerRegion is the latest data,
         *       because we need to keep the most recently used checkpoint.
         */
        void cleanupCompletedCheckpointsIfNecessary() {
            if (shouldClear || completedCheckpoints.size() >= CLEAR_COMPLETED_CHECKPOINT_THRESHOLD) {
                doCleanup();
                shouldClear = false;
            }
        }
    }
}
