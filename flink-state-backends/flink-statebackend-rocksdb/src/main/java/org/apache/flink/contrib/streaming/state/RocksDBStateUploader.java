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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.speculativeframe.*;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Help class for uploading RocksDB state files.
 */
public class RocksDBStateUploader extends RocksDBStateDataTransfer {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateUploader.class);
	private static final int READ_BUFFER_SIZE = 16 * 1024;
	public static final String STATE_UPLOADING_SPECULATIVE_TASK_TYPE = "rocksdb_increment_state_upload";
	private final Histogram uploadHistogram;

	public RocksDBStateUploader(
			int numberOfTaskSnapshottingThreads,
			int numberSlots,
			MetricGroup metricGroup,
			SpeculativeTasksManager speculativeTasksManager,
			boolean speculativeUploadingEnabled) {

		super(speculativeUploadingEnabled ?
						// Since the speculative task only needs one executor,
						// this thread pool shared by all tasks in taskmanager,
						// and the size is numberSolts times the original size(In order to be as consistent as possible).
						numberOfTaskSnapshottingThreads * numberSlots : numberOfTaskSnapshottingThreads,
		        speculativeUploadingEnabled ? Optional.of(speculativeTasksManager) : Optional.empty());

		this.uploadHistogram = metricGroup.histogram("uploadStateRt", new DescriptiveStatisticsHistogram(500));
	}

	/**
	 * Upload all the files to checkpoint fileSystem using specified number of threads.
	 *
	 * @param files The files will be uploaded to checkpoint filesystem.
	 * @param checkpointStreamFactory The checkpoint streamFactory used to create outputstream.
	 *
	 * @throws Exception Thrown if can not upload all the files.
	 */
	public Map<StateHandleID, StreamStateHandle> uploadFilesToCheckpointFs(
		@Nonnull Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws Exception {

		Map<StateHandleID, StreamStateHandle> handles = new HashMap<>();

		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures =
			createUploadFutures(files, checkpointStreamFactory, closeableRegistry);

		try {
			FutureUtils.waitForAll(futures.values()).get();

			for (Map.Entry<StateHandleID, CompletableFuture<StreamStateHandle>> entry : futures.entrySet()) {
				handles.put(entry.getKey(), entry.getValue().get());
			}
		} catch (Exception e) {
			handleException(futures.values());
			if (e instanceof ExecutionException) {
				Throwable throwable = ExceptionUtils.stripExecutionException(e);
				throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
				if (throwable instanceof IOException) {
					throw (IOException) throwable;
				} else {
					throw new FlinkRuntimeException("Failed to upload data for state handles.", e);
				}
			}
		}

		return handles;
	}

	private void handleException(Collection<CompletableFuture<StreamStateHandle>> futures) {
		getExecutorService().execute(
				() -> futures.forEach(
						future -> future.whenComplete((state, throwable) -> {
							if (throwable == null && state instanceof FileStateHandle) {
								try {
									state.discardStateWithRetry();
								} catch (Exception ex) {
									LOG.warn("Delete state file error", ex);
								}
							}
						})));
	}

	private Map<StateHandleID, CompletableFuture<StreamStateHandle>> createUploadFutures(
		Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) {
		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures = new HashMap<>(files.size());

		for (Map.Entry<StateHandleID, Path> entry : files.entrySet()) {
			futures.put(
					entry.getKey(),
					getUploadFuture(
							entry.getValue(),
							checkpointStreamFactory,
							closeableRegistry));
		}

		return futures;
	}

	/**
	 * Get a future based on whether enabled speculative execution uploading.
	 * @param filePath path
	 * @param checkpointStreamFactory checkpointStreamFactory
	 * @param closeableRegistry closeableRegistry
	 * @return future
	 */
	private CompletableFuture<StreamStateHandle> getUploadFuture(
			Path filePath,
			CheckpointStreamFactory checkpointStreamFactory,
			CloseableRegistry closeableRegistry) {
		CompletableFuture<StreamStateHandle> future;
		if (speculativeTasksManager.isPresent()) {
			future = speculatedUploadLocalFileToCheckpointFs(
					filePath,
					checkpointStreamFactory,
					closeableRegistry);
		} else {
			Supplier<StreamStateHandle> supplier =
					CheckedSupplier.unchecked(
							() -> {
								CheckpointStreamFactory.CheckpointStateOutputStream outputStream  =
										checkpointStreamFactory.createCheckpointStateOutputStream(
												CheckpointedStateScope.SHARED);
								closeableRegistry.registerCloseable(outputStream);
								return uploadLocalFileToCheckpointFs(
										filePath,
										outputStream,
										closeableRegistry);
							});
			future = CompletableFuture.supplyAsync(supplier, getExecutorService());
		}
		return future;
	}

	private CompletableFuture<StreamStateHandle> speculatedUploadLocalFileToCheckpointFs(
			Path filePath,
			CheckpointStreamFactory checkpointStreamFactory,
			CloseableRegistry closeableRegistry) {
		CompletableFuture<StreamStateHandle> future = new CompletableFuture<>();
		SpeculativeTaskInfo task = createUploadTask(
				filePath,
				checkpointStreamFactory,
				closeableRegistry,
				future);
		submitTransferTask(task);
		return future;
	}

	@Override
	public SpeculativeProperties buildTaskProperties() {
		return new SpeculativeProperties.SpeculativePropertiesBuilder(STATE_UPLOADING_SPECULATIVE_TASK_TYPE)
				.setScope(SpeculativeScope.TaskManager)
				.setThresholdType(SpeculativeThresholdType.NINETY_NINE)
				.setMinNumDataPoints(100)
				.setCustomMinThreshold(3000L)
				.setExecutor(getExecutorService())
				.build();
	}

	private SpeculativeTaskInfo createUploadTask(
			Path filePath,
			CheckpointStreamFactory checkpointStreamFactory,
			CloseableRegistry closeableRegistry,
			CompletableFuture<StreamStateHandle> future) {
		OutputStreamHolder holder = new OutputStreamHolder();
		return new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
				STATE_UPLOADING_SPECULATIVE_TASK_TYPE,
				unused -> {
					try {
						CheckpointStreamFactory.CheckpointStateOutputStream outputStream =
								checkpointStreamFactory.createInterruptableCheckpointStateOutputStream(
										CheckpointedStateScope.SHARED);
						// for cancel
						holder.setOutputStream(outputStream);
						closeableRegistry.registerCloseable(outputStream);

						return uploadLocalFileToCheckpointFs(
								filePath,
								outputStream,
								closeableRegistry);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.setDuplicatedTaskSucceedsCallback(result -> {
					StreamStateHandle stateHandle = (StreamStateHandle) result;
					try {
						stateHandle.discardStateWithRetry();
					} catch (Exception e) {
						LOG.warn("Fail to delete duplicated state file {}, it will still remain in DFS!",
							((FileStateHandle) stateHandle).getFilePath());
					}
				})
				.setTaskCanceller(taskFuture -> holder.cancelOutputStream())
				.setSucceedCallback(result -> future.complete((StreamStateHandle) result))
				.setFailCallback(taskFailureInfo -> {
					if (taskFailureInfo.isOriginalTaskFailed() &&
							(!taskFailureInfo.isSpeculatedTaskSubmitted() || taskFailureInfo.isSpeculatedTaskFailed())) {
						future.completeExceptionally(taskFailureInfo.getExecutionException());
					}
				})
				.build();
	}

	private StreamStateHandle uploadLocalFileToCheckpointFs(
			Path filePath,
			CheckpointStreamFactory.CheckpointStateOutputStream outputStream,
			CloseableRegistry closeableRegistry) throws IOException {

		InputStream inputStream = null;

		try {
			long uploadStart = System.currentTimeMillis();
			final byte[] buffer = new byte[READ_BUFFER_SIZE];

			inputStream = Files.newInputStream(filePath);
			closeableRegistry.registerCloseable(inputStream);

			while (true) {
				int numBytes = inputStream.read(buffer);

				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}
			long uploadCost = System.currentTimeMillis() - uploadStart;
			if (uploadCost > LOG_THRESHOLD_MS) {
				LOG.warn("upload increment local state file {} cost {}ms", filePath, uploadCost);
			}
			markCost(uploadHistogram, uploadCost);

			StreamStateHandle result = null;
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				result = outputStream.closeAndGetHandle();
				outputStream = null;
			}
			return result;

		} finally {

			if (closeableRegistry.unregisterCloseable(inputStream)) {
				IOUtils.closeQuietly(inputStream);
			}

			if (closeableRegistry.unregisterCloseable(outputStream)) {
				IOUtils.closeQuietly(outputStream);
			}
		}
	}

	private static class OutputStreamHolder {
		FsCheckpointStreamFactory.InterruptableFsCheckpointStateOutputStream outputStream;

		private void setOutputStream(CheckpointStreamFactory.CheckpointStateOutputStream outputStream) {
			this.outputStream =
					(FsCheckpointStreamFactory.InterruptableFsCheckpointStateOutputStream) outputStream;
		}

		private void cancelOutputStream() {
			if (outputStream != null) {
				outputStream.interrupt();
			}
		}
	}
}

