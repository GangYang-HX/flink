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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;

import javax.annotation.Nullable;

/**
 * Interface that handles checkpoint related messages, and decides how to handle the checkpoint result.
 */
public interface CheckpointHandler {

    boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message,
            String taskManagerLocationInfo,
            PendingCheckpoint checkpoint)
            throws CheckpointException;

    void receiveDeclineMessage(
            DeclineCheckpoint message,
            String taskManagerLocationInfo,
            @Nullable PendingCheckpoint checkpoint)
            throws CheckpointException;

    void processFailedTask(ExecutionAttemptID executionAttemptId, Throwable cause)
            throws CheckpointException;

    void sendAcknowledgeMessages(long checkpointId, long timestamp);

    void expirePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException;

    void processDeclineReason(CheckpointFailureManager failureManager, Throwable reason, long checkpointId);
}
