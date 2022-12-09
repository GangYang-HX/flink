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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.clock.Clock;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link CheckpointBarrierHandler} reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
public abstract class CheckpointBarrierHandler implements Closeable {

	/** The listener to be notified on complete checkpoints. */
	private final AbstractInvokable toNotifyOnCheckpoint;

    private final Clock clock;

	private long latestCheckpointStartDelayNanos;

	public CheckpointBarrierHandler(AbstractInvokable toNotifyOnCheckpoint, Clock clock) {
		this.toNotifyOnCheckpoint = checkNotNull(toNotifyOnCheckpoint);
        this.clock = checkNotNull(clock);
	}

	@Override
	public void close() throws IOException {
	}

	public abstract void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws IOException;

	public abstract void processBarrierAnnouncement(
			CheckpointBarrier announcedBarrier,
			int sequenceNumber,
			InputChannelInfo channelInfo) throws IOException;

	public abstract void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws IOException;

	public abstract void processEndOfPartition() throws IOException;

	public abstract void processChannelUnavailable(InputChannelInfo channelInfo);

	public abstract long getLatestCheckpointId();

	public long getAlignmentDurationNanos() {
//        if (isDuringAlignment()) {
//            return clock.relativeTimeNanos() - startOfAlignmentTimestamp;
//        } else {
//            return FutureUtils.getOrDefault(latestAlignmentDurationNanos, 0L);
//        }
		return 0;
	}

	public long getCheckpointStartDelayNanos() {
		return latestCheckpointStartDelayNanos;
	}

	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		return CompletableFuture.completedFuture(null);
	}

	protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier, long alignmentDurationNanos) throws IOException {
		CheckpointMetaData checkpointMetaData =
			new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

		CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
			.setAlignmentDurationNanos(alignmentDurationNanos)
			.setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);
		toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
			checkpointMetaData,
			checkpointBarrier.getCheckpointOptions(),
			checkpointMetrics);
	}

	protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
		notifyAbort(checkpointId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
		toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
	}

	protected void markCheckpointStart(long checkpointCreationTimestamp) {
		latestCheckpointStartDelayNanos = 1_000_000 * Math.max(
			0,
			System.currentTimeMillis() - checkpointCreationTimestamp);
	}

	protected abstract boolean isCheckpointPending();


	protected final Clock getClock() {
		return clock;
	}

	/** A handle to a delayed action which can be cancelled. */
	interface Cancellable {
		void cancel();
	}

}
