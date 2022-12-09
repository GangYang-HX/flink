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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

public class AcknowledgeTriggerSnapshot extends AbstractCheckpointMessage {

    private static final long serialVersionUID = -7873010060566515057L;

    private final long delay;

    public AcknowledgeTriggerSnapshot(
            JobID job,
            ExecutionAttemptID taskExecutionId,
            long checkpointId,
            long delay) {

        super(job, taskExecutionId, checkpointId);

        this.delay = delay;
    }


    public long getDelay() {
        return delay;
    }

    @Override
    public String toString() {
        return String.format(
                "Acknowledge Trigger Snapshot %d for (%s/%s) after %s ms",
                getCheckpointId(),
                getJob(),
                getTaskExecutionId(),
                delay);
    }
}
