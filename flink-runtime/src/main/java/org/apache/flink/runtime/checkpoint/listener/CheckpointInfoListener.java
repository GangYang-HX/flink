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

/**
 * The CheckpointInfoListener interface. All methods of this interface will be asynchronous executed
 * in class {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}.
 */
public interface CheckpointInfoListener {

    /**
     * Notify listener checkpoint has been triggered.
     *
     * @param checkpointInfo checkpoint information
     */
    void notifyCheckpointTriggered(CheckpointInfo checkpointInfo);

    /**
     * The implementers can define how to handle successful checkpoint information.
     *
     * @param checkpointInfo received successful checkpoint information
     */
    void notifyCheckpointComplete(CheckpointInfo checkpointInfo);

    /**
     * The implementers can define how to handle decline checkpoint information and failure reason.
     *
     * @param checkpointInfo checkpoint information
     * @param reason failure reason
     */
    void notifyCheckpointFailure(CheckpointInfo checkpointInfo, Throwable reason);

    /**
     * Checkpoint expired before complete.
     *
     * @param checkpointInfo checkpoint information
     */
    void notifyCheckpointExpiration(CheckpointInfo checkpointInfo);

    /**
     * Checkpoint was discarded.
     *
     * @param checkpointInfo checkpoint information
     */
    void notifyCheckpointDiscarded(CheckpointInfo checkpointInfo);
}
