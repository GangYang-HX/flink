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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link PipelinedSubpartition} used when runtime network retry is enabled is enabled.
 */
public class PipelinedNetworkRetrySubpartition extends PipelinedSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedNetworkRetrySubpartition.class);

	@GuardedBy("buffers")
	protected boolean available = true;

	@GuardedBy("buffers")
	private boolean partialBufferCleanupRequired = false;

	PipelinedNetworkRetrySubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public PipelinedSubpartitionView createReadView(
		boolean isRetry,
		BufferAvailabilityListener availabilityListener) {
		synchronized (buffers) {
			checkState(!isReleased);
			if (!isRetry) {
				checkState(readView == null,
					"Subpartition %s of is being (or already has been) consumed, " +
						"but pipelined subpartitions can only be consumed once.",
					getSubPartitionIndex(),
					parent.getPartitionId());

				readView = new PipelinedNetworkRetrySubpartitionView(this, availabilityListener);
			} else {
				LOG.info("Create a new subpartitionView for the retry request.");

				readView.releaseAllResources();

				checkState(
					readView.isReleased(),
					"Subpartition view should have already been released when the retrying " +
						"request issued.");

//				available = true;
				partialBufferCleanupRequired = true;
				readView = new PipelinedNetworkRetrySubpartitionView(this, availabilityListener);

				parent.notifyReadViewCreated(getSubPartitionIndex());
			}
		}
		return readView;
	}

	@Override
	public void consumerUnavailable() {
		LOG.info(
			"{}: Setting `available` to `false` for subpartition {} of partition {}.",
			parent.getOwningTaskName(),
			getSubPartitionIndex(),
			parent.getPartitionId());
		synchronized (buffers) {
			// Usually, the network reconnection is very fast, so we do not turn on the switch `available`,
			// which is used to drop the data.
//			available = false;
			isBlocked = false;
			partialBufferCleanupRequired = true;
			sequenceNumber = 0; // Reset the sequenceNumber.
		}
	}

	@Override
	public boolean isAvailable() {
		synchronized (buffers) {
			return available;
		}
	}

	@Override
	public void forceResumeConsumption() {
		synchronized (buffers) {
			isBlocked = false;
		}
	}

	@Override
	public void resumeConsumption() {
		synchronized (buffers) {
			// During unavailable, the barrier may be missed, causing `isBlocked` to be still false.
			// So we should skip the state checking.
			if (!available) {
				LOG.info("ResumeConsumption called when available is false, skip it, isBlocked is {}",
					isBlocked);
				isBlocked = false;
				return;
			}

			checkState(isBlocked, "Should be blocked by checkpoint.");

			isBlocked = false;
		}
	}

	public Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
		if (partialBufferCleanupRequired) {
			partialBufferCleanupRequired = !buffer.cleanupPartialRecord();
			if (!partialBufferCleanupRequired) {
				LOG.info("{} - {}: partialBufferCleanupRequired set to false.", parent.getOwningTaskName(), this);
			}
		}
		return buffer.build();
	}

	@Override
	protected int add(BufferConsumer bufferConsumer, int partialRecordLength, boolean finish) {
		synchronized (buffers) {
			if (!available) {
				bufferConsumer.close();
				return Integer.MAX_VALUE;
			}
		}

		return super.add(bufferConsumer, partialRecordLength, finish);
	}
}
