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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.speculativeframe.SpeculativeProperties;
import org.apache.flink.runtime.speculativeframe.SpeculativeTaskInfo;
import org.apache.flink.runtime.speculativeframe.SpeculativeTasksManager;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.concurrent.Executors.newDirectExecutorService;

/**
 * Data transfer base class for {@link RocksDBKeyedStateBackend}.
 */
abstract class RocksDBStateDataTransfer implements Closeable {

	protected static final int LOG_THRESHOLD_MS = 10_000;
	private final ExecutorService taskExecutorService;
	private final Object lock = new Object();
	protected Optional<SpeculativeTasksManager> speculativeTasksManager;


	RocksDBStateDataTransfer(
			int threadNum,
			Optional<SpeculativeTasksManager> speculativeTasksManager) {
		this.speculativeTasksManager = speculativeTasksManager;
		if (speculativeTasksManager.isPresent()) {
			taskExecutorService = ExecutorServiceHolder.getExecutor(threadNum);

			// register task type
			registerSpeculativeTask();
		} else {
			if (threadNum > 1) {
				taskExecutorService = Executors.newFixedThreadPool(threadNum);
			} else {
				taskExecutorService = newDirectExecutorService();
			}
		}
	}

	public abstract SpeculativeProperties buildTaskProperties();

	private void registerSpeculativeTask() {
		SpeculativeProperties taskProperties = buildTaskProperties();
		Preconditions.checkNotNull(taskProperties, "taskProperties is null");
		Preconditions.checkArgument(speculativeTasksManager.isPresent());
		speculativeTasksManager.get().registerTaskType(taskProperties);
	}

	protected void markCost(Histogram metrics, long duration) {
		//thread safe
		synchronized (lock) {
			metrics.update(duration);
		}
	}

	protected void submitTransferTask(SpeculativeTaskInfo taskInfo) {
		Preconditions.checkArgument(speculativeTasksManager.isPresent());
		speculativeTasksManager.get().submitTask(taskInfo);
	}

	protected ExecutorService getExecutorService() {
		return taskExecutorService;
	}

	@Override
	public void close() {
		if (taskExecutorService != null) {
			// Since there is only one executor for downloading and uploading, we needn't close it.
			if (!speculativeTasksManager.isPresent()) {
				taskExecutorService.shutdownNow();
			}
		}
	}

	private static class ExecutorServiceHolder {

		private static volatile ExecutorService transferExecutor;

		private static ExecutorService getExecutor(int threadNum) {
			if (transferExecutor == null) {
				synchronized (ExecutorServiceHolder.class) {
					if (transferExecutor == null) {
						transferExecutor = Executors.newFixedThreadPool(threadNum);
					}
				}
			}
			return transferExecutor;
		}
	}
}
