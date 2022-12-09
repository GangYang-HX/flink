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

package org.apache.flink.runtime.checkpoint.listener;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * CheckpointInfoFactory provides ability to get {@link CheckpointInfo}. These methods are synchronous
 * executed in {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}. If they take too much time,
 * this will cause the checkpoint to timeout.
 */
public class CheckpointInfoFactory {

    public CheckpointInfo createCompletedCheckpointInfo(
            PendingCheckpoint pendingCheckpoint,
            Map<OperatorID, OperatorState> operatorStates,
            long checkpointEndTime,
            boolean isRegional) {
        CheckpointInfo checkpointInfo = createCommonCheckpointInfo(pendingCheckpoint);
        List<CheckpointInfo.StateInfo> stateInfos = new ArrayList<>();
        Preconditions.checkNotNull(operatorStates, "operatorStates can't be null.");
        for (Map.Entry<OperatorID, OperatorState> entry : operatorStates.entrySet()) {
            for (OperatorSubtaskState states : entry.getValue().getStates()) {
                // managed keyed state
                if (states.getManagedKeyedState().hasState()) {
                    states.getManagedKeyedState().forEach(keyedStateHandle -> {
                        // IncrementalRemoteKeyedStateHandle
                        if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                            IncrementalRemoteKeyedStateHandle stateHandle =
                                    (IncrementalRemoteKeyedStateHandle) keyedStateHandle;
                            // metaStateHandle
                            handleMetaStateHandle(stateHandle).ifPresent(stateInfos::add);
                            // privateState and sharedState
                            stateInfos.addAll(handleStates(stateHandle));
                        }
                    });
                }
                // managed operator state
                if (states.getManagedOperatorState().hasState()) {
                    states.getManagedOperatorState().forEach(operatorStateHandle ->
                            stateInfos.add(packageStateInfo(
                                    operatorStateHandle.getDelegateStateHandle(),
                                    CheckpointInfo.StateType.OPERATOR)));
                }
                // input channel state && result subpartition channel state
                if (states.getInputChannelState().hasState() || states.getResultSubpartitionState().hasState()) {
                    states.getInputChannelState().forEach(inputChannelStateHandle ->
                            stateInfos.add(packageStateInfo(
                                    inputChannelStateHandle.getDelegate(),
                                    CheckpointInfo.StateType.CHANNEL)));
                    states.getResultSubpartitionState().forEach(resultSubpartitionStateHandle -> stateInfos.add(
                            packageStateInfo(
                                    resultSubpartitionStateHandle.getDelegate(),
                                    CheckpointInfo.StateType.CHANNEL)));
                }
            }
        }

        String metadataFileFullPath = pendingCheckpoint.getCheckpointStorageLocation().getMetadataFileFullPath();
        return checkpointInfo
                .setCheckpointEndTime(checkpointEndTime)
                .setMetadataFileFullPath(metadataFileFullPath)
                .setResult(isRegional ? CheckpointInfo.CheckpointResult.COMPLETED_REGIONAL :
                        CheckpointInfo.CheckpointResult.COMPLETED_GLOBAL)
                .setStates(Collections.emptyList())
                .setStates(stateInfos);
    }

    public CheckpointInfo createFailedCheckpointInfo(
            PendingCheckpoint pendingCheckpoint,
            CheckpointFailureReason reason) {
        CheckpointInfo checkpointInfo = createCommonCheckpointInfo(pendingCheckpoint);
        return checkpointInfo
                .setCheckpointEndTime(System.currentTimeMillis())
                .setResult(CheckpointInfo.CheckpointResult.FAILED)
                .setFailureReason(reason);
    }

    public CheckpointInfo createExpiredCheckpointInfo(PendingCheckpoint pendingCheckpoint) {
        CheckpointInfo checkpointInfo = createCommonCheckpointInfo(pendingCheckpoint);
        return checkpointInfo
                .setCheckpointEndTime(System.currentTimeMillis())
                .setResult(CheckpointInfo.CheckpointResult.CANCELED)
                .setFailureReason(CheckpointFailureReason.CHECKPOINT_EXPIRED);
    }

    public CheckpointInfo createDiscardedCheckpointInfo(CompletedCheckpoint completedCheckpoint) {
        CheckpointInfo checkpointInfo = createCommonCheckpointInfo(completedCheckpoint);
        String metadataFileFullPath = completedCheckpoint.getStorageLocation().getMetadataFileFullPath();
        return checkpointInfo
                .setCheckpointEndTime(completedCheckpoint.getTimestamp() + completedCheckpoint.getDuration())
                .setResult(CheckpointInfo.CheckpointResult.DISCARDED)
                .setMetadataFileFullPath(metadataFileFullPath);
    }

    public CheckpointInfo createTriggeredCheckpointInfo(PendingCheckpoint pendingCheckpoint) {
        CheckpointInfo checkpointInfo = createCommonCheckpointInfo(pendingCheckpoint);
        return checkpointInfo
                .setMetadataFileFullPath(pendingCheckpoint.getCheckpointStorageLocation().getMetadataFileFullPath());
    }

    private <T> CheckpointInfo createCommonCheckpointInfo(T t) {
        String jobId = null;
        long checkpointId = 0;
        long checkpointTriggerTime = 0;
        CheckpointInfo.OuterCheckpointType outerCheckpointType = CheckpointInfo.OuterCheckpointType.CHECKPOINT;
        if (t instanceof PendingCheckpoint) {
            jobId = ((PendingCheckpoint) t).getJobId().toString();
            checkpointId = ((PendingCheckpoint) t).getCheckpointId();
            checkpointTriggerTime = ((PendingCheckpoint) t).getCheckpointTimestamp();
            if (((PendingCheckpoint) t).getProps().isSavepoint()) {
                outerCheckpointType = CheckpointInfo.OuterCheckpointType.SAVEPOINT;
            } else if (((PendingCheckpoint) t).getProps().isFull()) {
                outerCheckpointType = CheckpointInfo.OuterCheckpointType.FULL_CHECKPOINT;
            }
        } else if (t instanceof CompletedCheckpoint) {
            jobId = ((CompletedCheckpoint) t).getJobId().toString();
            checkpointId = ((CompletedCheckpoint) t).getCheckpointID();
            checkpointTriggerTime = ((CompletedCheckpoint) t).getTimestamp();
            if (((CompletedCheckpoint) t).getProperties().isSavepoint()) {
                outerCheckpointType = CheckpointInfo.OuterCheckpointType.SAVEPOINT;
            } else if (((CompletedCheckpoint) t).getProperties().isFull()) {
                outerCheckpointType = CheckpointInfo.OuterCheckpointType.FULL_CHECKPOINT;
            }
        }
        return new CheckpointInfo(jobId, checkpointId, checkpointTriggerTime, outerCheckpointType);
    }

    private Optional<CheckpointInfo.StateInfo> handleMetaStateHandle(IncrementalRemoteKeyedStateHandle stateHandle) {
        if (stateHandle.getMetaStateHandle() == null) {
            return Optional.empty();
        }
        return Optional.of(packageStateInfo(stateHandle.getMetaStateHandle(), CheckpointInfo.StateType.KEYED));
    }

    private List<CheckpointInfo.StateInfo> handleStates(IncrementalRemoteKeyedStateHandle stateHandle) {
        List<CheckpointInfo.StateInfo> states = new ArrayList<>();
        stateHandle.getPrivateState().forEach((stateHandleID, streamStateHandle) ->
                states.add(packageStateInfo(streamStateHandle, CheckpointInfo.StateType.KEYED)));
        stateHandle.getSharedState().forEach((stateHandleID, streamStateHandle) ->
                states.add(packageStateInfo(streamStateHandle, CheckpointInfo.StateType.KEYED)));
        return states;
    }

    private CheckpointInfo.StateInfo packageStateInfo(StreamStateHandle stateHandle, CheckpointInfo.StateType type) {
        Path path = null;
        if (stateHandle instanceof FileStateHandle) {
            path = ((FileStateHandle) stateHandle).getFilePath();
        }
        return new CheckpointInfo.StateInfo(type, path, stateHandle.getStateSize());
    }
}
