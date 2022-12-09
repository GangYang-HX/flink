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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * The CheckpointInfo is used to collect flink checkpoint information. It is mainly used to send flink checkpoint
 * information to external systems. It is worth noting that OperatorState is a nested structure, and states are the
 * result of flattening them. For some jobs, the number of elements in the states list may be very large, and the
 * implementer of the Listener needs to be careful not to hold the reference of the list for a long time to prevent
 * the gc pressure of the job manager.
 */
public class CheckpointInfo {

    private final String jobId;

    private final long checkpointId;

    private final long checkpointTriggerTime;

    private long checkpointEndTime;

    /**
     * The external system can judge the type (checkpoint、full checkpoint or savepoint) through this field.
     */
    private OuterCheckpointType outerCheckpointType;

    /**
     * MetadataFileFullPath will set the metadata path (e.g. XXX/_metadata) if the checkpoint was triggered、completed
     * or discarded, otherwise it will be set to null.
     */
    @Nullable
    private String metadataFileFullPath;

    private CheckpointResult result;

    @Nullable
    private CheckpointFailureReason failureReason;

    private List<StateInfo> states;

    public CheckpointInfo(
            String jobId,
            long checkpointId,
            long checkpointTriggerTime,
            OuterCheckpointType outerCheckpointType) {
        this(
            jobId,
            checkpointId,
            checkpointTriggerTime,
            Long.MAX_VALUE,
            outerCheckpointType,
            null,
            CheckpointResult.UNKNOWN,
            null,
            Collections.emptyList());
    }

    public CheckpointInfo(
            String jobId,
            long checkpointId,
            long checkpointTriggerTime,
            long checkpointEndTime,
            OuterCheckpointType outerCheckpointType,
            String metadataFileFullPath,
            CheckpointResult result,
            CheckpointFailureReason failureReason,
            List<StateInfo> states) {
        this.jobId = jobId;
        this.checkpointId = checkpointId;
        this.checkpointTriggerTime = checkpointTriggerTime;
        this.checkpointEndTime = checkpointEndTime;
        this.outerCheckpointType = outerCheckpointType;
        this.metadataFileFullPath = metadataFileFullPath;
        this.result = result;
        this.failureReason = failureReason;
        this.states = states;
    }

    public String getJobId() {
        return jobId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getCheckpointTriggerTime() {
        return checkpointTriggerTime;
    }

    public long getCheckpointEndTime() {
        return checkpointEndTime;
    }

    public String getMetadataFileFullPath() {
        return metadataFileFullPath;
    }

    public CheckpointResult getResult() {
        return result;
    }

    public CheckpointFailureReason getFailureReason() {
        return failureReason;
    }

    public List<StateInfo> getStates() {
        return states;
    }

    public CheckpointInfo setCheckpointEndTime(long checkpointEndTime) {
        this.checkpointEndTime = checkpointEndTime;
        return this;
    }

    public CheckpointInfo setMetadataFileFullPath(String metadataFileFullPath) {
        this.metadataFileFullPath = metadataFileFullPath;
        return this;
    }

    public CheckpointInfo setResult(CheckpointResult result) {
        this.result = result;
        return this;
    }

    public CheckpointInfo setFailureReason(CheckpointFailureReason failureReason) {
        this.failureReason = failureReason;
        return this;
    }

    public CheckpointInfo setStates(List<StateInfo> states) {
        this.states = states;
        return this;
    }

    public OuterCheckpointType getOuterCheckpointType() {
        return outerCheckpointType;
    }

    /**
     * StateInfo provides flink state basic information. It's worth noting that not all state have state paths.
     * Because if the state size is small, which will be saved in metadata.
     */
    public static class StateInfo {
        private final StateType type;
        @Nullable
        private final Path statePath;
        private final long stateSize;

        public StateInfo(StateType type, @Nullable Path statePath, long stateSize) {
            this.type = type;
            this.statePath = statePath;
            this.stateSize = stateSize;
        }

        public StateType getType() {
            return type;
        }

        @Nullable
        public Path getStatePath() {
            return statePath;
        }

        public long getStateSize() {
            return stateSize;
        }
    }

    public enum CheckpointResult {
        COMPLETED_GLOBAL, // completed for a global checkpoint
        COMPLETED_REGIONAL, // completed for a regional checkpoint
        FAILED, // failed for a global/regional checkpoint
        CANCELED, // canceled for a global/regional checkpoint
        DISCARDED, // discarded checkpoint
        UNKNOWN // unknown status
    }

    public enum StateType {
        KEYED, OPERATOR, CHANNEL
    }

    /**
     * A checkpoint type that only applies to external systems, to distinguish between savepoint、full checkpoint and checkpoint.
     */
    public enum OuterCheckpointType {
        CHECKPOINT, FULL_CHECKPOINT, SAVEPOINT
    }
}
