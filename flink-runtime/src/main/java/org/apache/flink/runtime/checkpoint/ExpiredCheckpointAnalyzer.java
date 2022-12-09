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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.flink.runtime.checkpoint.ExpiredType.BARRIER_TRIGGER_EXPIRED;
import static org.apache.flink.runtime.checkpoint.ExpiredType.SNAPSHOT_EXPIRED;

@NotThreadSafe
public class ExpiredCheckpointAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(ExpiredCheckpointAnalyzer.class);
    private final long numOfTotalTasks;
    private long maxDelay;
    private long receivedTasks;

    public ExpiredCheckpointAnalyzer(long numOfTotalTasks, long numOfSourceTasks) {
        this.numOfTotalTasks = numOfTotalTasks;
        // Set the initial value to be the number of source tasks,
        // because the source tasks will not ack barrier trigger signal forever.
        this.receivedTasks = numOfSourceTasks;
    }

    public void acknowledgeTriggerSnapshot(long delay) {
        maxDelay = Math.max(maxDelay, delay);
        ++receivedTasks;
    }

    public CheckpointException getExpiredExceptionWithDetail() {
        String additionalMsg;
        if (receivedTasks == numOfTotalTasks) {
            additionalMsg = SNAPSHOT_EXPIRED.getMessageProvider().getMessage(String.valueOf(maxDelay));
        } else {
            // not all task started snapshot
            additionalMsg = BARRIER_TRIGGER_EXPIRED.getMessageProvider()
                    .getMessage(String.valueOf(receivedTasks), String.valueOf(numOfTotalTasks));
        }
        return new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED, additionalMsg);
    }

}
