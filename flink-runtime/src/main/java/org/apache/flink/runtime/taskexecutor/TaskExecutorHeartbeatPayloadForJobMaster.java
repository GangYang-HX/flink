/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.taskexecutor.slot.SlotOfferReport;
import org.apache.flink.runtime.taskmanager.TaskExecutionStateReport;

import java.io.Serializable;

/**
 * Payload for heartbeats sent from the TaskExecutor to the JobMaster.
 */
public class TaskExecutorHeartbeatPayloadForJobMaster implements Serializable {

	private static final long serialVersionUID = 1L;

	private final AccumulatorReport accumulatorReport;
	private final SlotOfferReport slotOfferReport;
	private final TaskExecutionStateReport taskExecutionStateReport;

	public TaskExecutorHeartbeatPayloadForJobMaster() {
		this.accumulatorReport = new AccumulatorReport();
		this.slotOfferReport = new SlotOfferReport();
		this.taskExecutionStateReport = new TaskExecutionStateReport();
	}

	public TaskExecutorHeartbeatPayloadForJobMaster(
		AccumulatorReport accumulatorReport,
		SlotOfferReport slotOfferReport,
		TaskExecutionStateReport taskExecutionStateReport) {
		this.accumulatorReport = accumulatorReport;
		this.slotOfferReport = slotOfferReport;
		this.taskExecutionStateReport = taskExecutionStateReport;
	}

	public AccumulatorReport getAccumulatorReport() {
		return accumulatorReport;
	}

	public TaskExecutionStateReport getTaskExecutionStateReport() {
		return taskExecutionStateReport;
	}

	public SlotOfferReport getSlotOfferReport() {
		return slotOfferReport;
	}

	@Override
	public String toString() {
		return "TaskExecutorToJobMasterHeartbeatPayload{" +
			"accumulatorReport=" + accumulatorReport +
			", taskExecutionStateReport=" + taskExecutionStateReport +
			", slotOfferReport=" + slotOfferReport +
			'}';
	}

	public static PayloadDescriptor createPayloadDescriptor(
		boolean hasAccumulatorReport,
		boolean hasSlotOfferReport,
		boolean hasTaskExecutionStateReport) {

		return new PayloadDescriptor(
			hasAccumulatorReport, hasSlotOfferReport, hasTaskExecutionStateReport);
	}

	/**
	 * Payload descriptor which is used to create a {@link TaskExecutorHeartbeatPayloadForJobMaster} instance.
	 */
	static class PayloadDescriptor {
		/** Default heartbeat payload only has AccumulatorReport. */
		public static final PayloadDescriptor DEFAULT = new PayloadDescriptor(true, false, false);

		/** Sync slot report and execution state to jobMaster. */
		public static final PayloadDescriptor SYNC_STATUS = new PayloadDescriptor(true, true, true);

		private boolean hasAccumulatorReport;
		private boolean hasSlotOfferReport;
		private boolean hasTaskExecutionStateReport;

		public PayloadDescriptor(
			boolean hasAccumulatorReport,
			boolean hasSlotOfferReport,
			boolean hasTaskExecutionStateReport) {
			this.hasAccumulatorReport = hasAccumulatorReport;
			this.hasSlotOfferReport = hasSlotOfferReport;
			this.hasTaskExecutionStateReport = hasTaskExecutionStateReport;
		}

		public boolean hasAccumulatorReport() {
			return hasAccumulatorReport;
		}

		public boolean hasSlotOfferReport() {
			return hasSlotOfferReport;
		}

		public boolean hasTaskExecutionStateReport() {
			return hasTaskExecutionStateReport;
		}
	}
}
