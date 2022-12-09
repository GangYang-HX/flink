/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

/**
 * Abstract base class for implementing {@link SnapshotStrategy}, that gives a consistent logging across state backends.
 *
 * @param <T> type of the snapshot result.
 */
public abstract class AbstractSnapshotStrategy<T extends StateObject> implements SnapshotStrategy<SnapshotResult<T>> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractSnapshotStrategy.class);

	private static final String LOG_SYNC_COMPLETED_TEMPLATE = "{} ({}, synchronous part) in thread {} took {} ms.";
	private static final String LOG_ASYNC_COMPLETED_TEMPLATE = "{} ({}, asynchronous part) in thread {} took {} ms.";

	/** Descriptive name of the snapshot strategy that will appear in the log outputs and {@link #toString()}. */
	@Nonnull
	protected final String description;

	@Nullable
	protected Collection<StateDescriptor> stateDescriptors;

	@Nullable
	protected OperatorID operatorID;

	@Nullable
	protected int subTaskIndex;

	private boolean stateDescriptorsPersisted;

	protected AbstractSnapshotStrategy(@Nonnull String description) {
		this.description = description;
	}

	/**
	 * Logs the duration of the synchronous snapshot part from the given start time.
	 */
	public void logSyncCompleted(@Nonnull Object checkpointOutDescription, long startTime) {
		logCompletedInternal(LOG_SYNC_COMPLETED_TEMPLATE, checkpointOutDescription, startTime);
	}

	/**
	 * Logs the duration of the asynchronous snapshot part from the given start time.
	 */
	public void logAsyncCompleted(@Nonnull Object checkpointOutDescription, long startTime) {
		logCompletedInternal(LOG_ASYNC_COMPLETED_TEMPLATE, checkpointOutDescription, startTime);
	}

	private void logCompletedInternal(
		@Nonnull String template,
		@Nonnull Object checkpointOutDescription,
		long startTime) {

		long duration = (System.currentTimeMillis() - startTime);

		LOG.debug(
			template,
			description,
			checkpointOutDescription,
			Thread.currentThread(),
			duration);
	}

	protected void snapshotStateDescriptor(CheckpointStreamFactory streamFactory, @Nullable TypeSerializer keySerializer) {

		if (subTaskIndex != 0 || stateDescriptorsPersisted || !(streamFactory instanceof FsCheckpointStreamFactory)) {
			//skip
			return;
		}
		CheckpointStreamFactory.CheckpointStateOutputStream checkpointStateDescOutputStream = null;
		try {
			checkpointStateDescOutputStream =
				streamFactory.createCheckpointStateDescOutputStream(operatorID);
			DataOutputView stateDescOutputView = new DataOutputViewStreamWrapper(checkpointStateDescOutputStream);
			StateDescriptorSerializationProxy stateDescriptorSerializationProxy =
				new StateDescriptorSerializationProxy(operatorID, stateDescriptors, keySerializer);
			stateDescriptorSerializationProxy.write(stateDescOutputView);
			checkpointStateDescOutputStream.flush();
			stateDescriptorsPersisted = true;
		} catch (IOException e) {
			LOG.error("snapshot state desc failed.", e);
		} finally {
			if (checkpointStateDescOutputStream != null) {
				try {
					checkpointStateDescOutputStream.close();
				} catch (IOException e) {
					LOG.error("close output stream while snapshot state desc failed.", e);
				}
			}
		}
	}

	public void setStateDescriptors(Collection<StateDescriptor> stateDescriptors) {
		this.stateDescriptors = stateDescriptors;
	}

	public void setOperatorID(@Nullable OperatorID operatorID) {
		this.operatorID = operatorID;
	}

	public void setSubTaskIndex(int subTaskIndex) {
		this.subTaskIndex = subTaskIndex;
	}

	@Override
	public String toString() {
		return "SnapshotStrategy {" + description + "}";
	}
}
