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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.speculativeframe.SpeculativeProperties;
import org.apache.flink.runtime.speculativeframe.SpeculativeScope;
import org.apache.flink.runtime.speculativeframe.SpeculativeTaskInfo;
import org.apache.flink.runtime.speculativeframe.SpeculativeTasksManager;
import org.apache.flink.runtime.speculativeframe.SpeculativeThresholdType;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Help class for downloading RocksDB state files.
 */
public class RocksDBStateDownloader extends RocksDBStateDataTransfer {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateDownloader.class);
	public static final String STATE_DOWNLOADING_SPECULATIVE_TASK_TYPE = "rocksdb_state_download";
	public static final String TARGET_FILE_TEMP_SUFFIX = ".speculativetemp";
	private final Histogram downloadHistogram;
	private final long timeout;

	public RocksDBStateDownloader(
			int restoringThreadNum,
			int numberSlots,
			long timeout,
			MetricGroup metricGroup,
			SpeculativeTasksManager speculativeTasksManager,
			boolean speculativeDownloadingEnabled) {

		super(speculativeDownloadingEnabled ?
						// Since the speculative task only needs one executor,
						// this thread pool shared by all tasks in taskmanager,
						// and the size is numberSolts times the original size(In order to be as consistent as possible).
						numberSlots * restoringThreadNum : restoringThreadNum,
				speculativeDownloadingEnabled ? Optional.of(speculativeTasksManager) : Optional.empty());

		this.timeout = timeout;
		this.downloadHistogram = metricGroup.histogram("downloadStateRt", new DescriptiveStatisticsHistogram(500));
	}

	/**
	 * Transfer all state data to the target directory using specified number of threads.
	 *
	 * @param restoreStateHandle Handles used to retrieve the state data.
	 * @param dest The target directory which the state data will be stored.
	 *
	 * @throws Exception Thrown if can not transfer all the state data.
	 */
	public void transferAllStateDataToDirectory(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path dest,
		CloseableRegistry closeableRegistry) throws Exception {

		final Map<StateHandleID, StreamStateHandle> sstFiles =
			restoreStateHandle.getSharedState();
		final Map<StateHandleID, StreamStateHandle> miscFiles =
			restoreStateHandle.getPrivateState();

		downloadDataForAllStateHandles(sstFiles, dest, closeableRegistry);
		downloadDataForAllStateHandles(miscFiles, dest, closeableRegistry);
	}

	/**
	 * Copies all the files from the given stream state handles to the given path, renaming the files w.r.t. their
	 * {@link StateHandleID}.
	 */
	private void downloadDataForAllStateHandles(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath,
		CloseableRegistry closeableRegistry) throws Exception {

		try {
			List<CompletableFuture<Void>> futures =
					createDownloadFutures(stateHandleMap, restoreInstancePath, closeableRegistry);
			//add time out for downloading
			FutureUtils.waitForAll(futures).get(timeout, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new TimeoutException("Failed to complete the download within the specified time (" + timeout + "ms)");
		} catch (ExecutionException e) {
			Throwable throwable = ExceptionUtils.stripExecutionException(e);
			throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
			if (throwable instanceof IOException) {
				throw (IOException) throwable;
			} else {
				throw new FlinkRuntimeException("Failed to download data for state handles.", e);
			}
		}
	}

	private CompletableFuture<Void> getDownloadFuture(
			Path path,
			StreamStateHandle remoteFileHandle,
			CloseableRegistry closeableRegistry) {
		if (speculativeTasksManager.isPresent()) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			submitTransferTask(createDownloadTask(path, remoteFileHandle, closeableRegistry, future));
			return future;
		} else {
			return CompletableFuture.runAsync(
					ThrowingRunnable.unchecked(
							() -> downloadDataForStateHandle(path, remoteFileHandle, closeableRegistry)),
					getExecutorService());
		}
	}

	private List<CompletableFuture<Void>> createDownloadFutures(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath,
		CloseableRegistry closeableRegistry) {
		List<CompletableFuture<Void>> futures = new ArrayList<>(stateHandleMap.size());
		for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
			StateHandleID stateHandleID = entry.getKey();
			StreamStateHandle remoteFileHandle = entry.getValue();

			Path path = restoreInstancePath.resolve(stateHandleID.toString());

			futures.add(getDownloadFuture(path, remoteFileHandle, closeableRegistry));
		}
		return futures;
	}

	@Override
	public SpeculativeProperties buildTaskProperties() {
		return new SpeculativeProperties.SpeculativePropertiesBuilder(STATE_DOWNLOADING_SPECULATIVE_TASK_TYPE)
				.setScope(SpeculativeScope.TaskManager)
				.setThresholdType(SpeculativeThresholdType.NINETY_NINE)
				// The download task will only be triggered when it is restored,
				// we can make the minNumDataPoints smaller to
				// use speculative execution as soon as possible.
				.setMinNumDataPoints(10)
				.setCustomMinThreshold(3000L)
				.setExecutor(getExecutorService())
				.build();
	}

	private SpeculativeTaskInfo createDownloadTask(
			Path targetPath,
			StreamStateHandle remoteFileHandle,
			CloseableRegistry closeableRegistry,
			CompletableFuture<Void> future) {
		return new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
				STATE_DOWNLOADING_SPECULATIVE_TASK_TYPE,
				tempPath -> {
					try {
						downloadDataForStateHandle((Path) tempPath, remoteFileHandle, closeableRegistry);
						return tempPath;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.setTaskDataGenerator(() -> {
					// Create a temporary file and rename it to the target file name
					// after the download is successful.
					String parent = targetPath.getParent().toString();
					String tempFileName =
							targetPath.getFileName().toString() + UUID.randomUUID() + TARGET_FILE_TEMP_SUFFIX;
					return Paths.get(parent, tempFileName);
				})
				.setTaskCanceller(taskFuture -> taskFuture.cancel(true))
				.setSucceedCallback(tempPath -> {
					// rename file
					File tempFile = ((Path) tempPath).toFile();
					if (tempFile.renameTo(targetPath.toFile())) {
						future.complete(null);
					} else {
						String errorMsg = String.format("Cannot rename file %s to %s", tempPath, targetPath);
						future.completeExceptionally(new IOException(errorMsg));
					}
				})
				.setDuplicatedTaskSucceedsCallback(tempPath -> dropFileIfExist((Path) tempPath))
				.setFailCallback(taskFailureInfo -> {
					if (taskFailureInfo.isOriginalTaskFailed() && taskFailureInfo.isSpeculatedTaskFailed()) {
						future.completeExceptionally(taskFailureInfo.getExecutionException());
					} else if (taskFailureInfo.isOriginalTaskFailed()) {
						dropFileIfExist((Path) taskFailureInfo.getOriginalTaskData());
					} else if (taskFailureInfo.isSpeculatedTaskFailed()) {
						dropFileIfExist((Path) taskFailureInfo.getSpeculatedTaskData());
					}
				})
				.build();

	}

	private void dropFileIfExist(Path path) {
		if (path == null) {
			return;
		}
		File file = path.toFile();
		if (file.exists()) {
			file.delete();
		}
	}

	/**
	 * Copies the file from a single state handle to the given path.
	 */
	private void downloadDataForStateHandle(
		Path restoreFilePath,
		StreamStateHandle remoteFileHandle,
		CloseableRegistry closeableRegistry) throws IOException {

		FSDataInputStream inputStream = null;
		OutputStream outputStream = null;

		try {
			long downloadStart = System.currentTimeMillis();
			inputStream = remoteFileHandle.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			Files.createDirectories(restoreFilePath.getParent());
			outputStream = Files.newOutputStream(restoreFilePath);
			closeableRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[8 * 1024];
			while (true) {
				int numBytes = inputStream.read(buffer);
				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}
			long downloadCost = System.currentTimeMillis() - downloadStart;
			long size = restoreFilePath.toAbsolutePath().toFile().length() / 1024 / 1024;
			if (downloadCost > LOG_THRESHOLD_MS) {
				LOG.warn("download state file {} cost {}ms, size {}MB",
						((FileStateHandle) remoteFileHandle).getFilePath(), downloadCost, size);
			}
			markCost(downloadHistogram, downloadCost);
		} finally {
			if (closeableRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}

			if (closeableRegistry.unregisterCloseable(outputStream)) {
				outputStream.close();
			}
		}
	}
}
