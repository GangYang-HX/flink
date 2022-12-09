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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.persistence.AbstractFileSystemReaderWriter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers with the FileSystem implement.
 *
 * <p>Each checkpoint creates a FileSystem path:
 * <pre>
 * +----O /flink/cluster-id/checkpoints/&lt;job-id&gt;/&lt;checkpoint-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/cluster-id/checkpoints/&lt;job-id&gt;/&lt;checkpoint-id&gt; N [persistent]
 * </pre>
 *
 * <p>During recovery, the latest checkpoint is read from a FileSystem. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number
 * of retained checkpoints is greater than one).
 */
public class FileSystemCompletedCheckpointStore extends AbstractFileSystemReaderWriter<CompletedCheckpoint>
	implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemCompletedCheckpointStore.class);

	private static final String CHECKPOINTS_PATH = "/checkpoints";

	private final Executor executor;

	private final Path pathPrefix;

	/**
	 * The maximum number of checkpoints to retain (at least 1).
	 */
	private final int maxNumberOfCheckpointsToRetain;

	/**
	 * Local copy of the completed checkpoints in FileSystem. This is restored from FileSystem
	 * when recovering and is maintained in parallel to the state in FileSystem during normal
	 * operations.
	 */
	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

	public FileSystemCompletedCheckpointStore(
		FileSystem fs,
		Configuration configuration,
		JobID jobID,
		Executor executor) {
		super(fs, configuration);
		this.executor = checkNotNull(executor, "Executor");

		this.pathPrefix = new Path(
			new Path(
				HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration),
				CHECKPOINTS_PATH),
			jobID.toString());

		this.maxNumberOfCheckpointsToRetain = configuration
			.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);
		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");

		this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return false;
	}

	/**
	 * Gets the latest checkpoint from FileSystem and removes all others.
	 */
	@Override
	public void recover() throws Exception {
		LOG.info("Recovering checkpoints from FileSystem.");

		if (!fs.exists(pathPrefix) || !fs.getFileStatus(pathPrefix).isDir()) {
			return;
		}

		// First get all checkpoint paths from file system
		FileStatus[] statuses = fs.listStatus(pathPrefix);
		List<Path> initialCheckpointPaths = Arrays.stream(statuses)
			.map(s -> s.getPath())
			.sorted(Comparator.comparing(Path::getPath))
			.collect(Collectors.toList());

		int numberOfInitialCheckpoints = initialCheckpointPaths.size();

		LOG.info("Found {} checkpoints in file system.", numberOfInitialCheckpoints);

		// Try and read the state handles from storage. We try until we either successfully read
		// all of them or when we reach a stable state, i.e. when we successfully read the same set
		// of checkpoints in two tries. We do it like this to protect against transient outages
		// of the checkpoint store (for example a DFS): if the DFS comes online midway through
		// reading a set of checkpoints we would run the risk of reading only a partial set
		// of checkpoints while we could in fact read the other checkpoints as well if we retried.
		// Waiting until a stable state protects against this while also being resilient against
		// checkpoints being actually unreadable.
		//
		// These considerations are also important in the scope of incremental checkpoints, where
		// we use ref-counting for shared state handles and might accidentally delete shared state
		// of checkpoints that we don't read due to transient storage outages.
		List<CompletedCheckpoint> lastTryRetrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		List<CompletedCheckpoint> retrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		do {
			LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

			lastTryRetrievedCheckpoints.clear();
			lastTryRetrievedCheckpoints.addAll(retrievedCheckpoints);
			retrievedCheckpoints.clear();

			for (Path path : initialCheckpointPaths) {
				try {
					CompletedCheckpoint completedCheckpoint = read(path);
					if (completedCheckpoint != null) {
						retrievedCheckpoints.add(completedCheckpoint);
					}
				} catch (Exception e) {
					LOG.warn("Could not retrieve checkpoint, not adding to list of recovered checkpoints.", e);
				}
			}

		} while (retrievedCheckpoints.size() != numberOfInitialCheckpoints &&
			!CompletedCheckpoint.checkpointsMatch(lastTryRetrievedCheckpoints, retrievedCheckpoints));

		completedCheckpoints.clear();
		completedCheckpoints.addAll(retrievedCheckpoints);

		if (completedCheckpoints.isEmpty() && numberOfInitialCheckpoints > 0) {
			throw new FlinkException(
				"Could not read any of the " + numberOfInitialCheckpoints + " checkpoints from storage.");
		} else if (completedCheckpoints.size() != numberOfInitialCheckpoints) {
			LOG.warn(
				"Could only fetch {} of {} checkpoints from storage.",
				completedCheckpoints.size(),
				numberOfInitialCheckpoints);
		}
	}

	/**
	 * Synchronously writes the new checkpoints to FileSystem and asynchronously removes older ones.
	 *
	 * @param checkpoint Completed checkpoint to add.
	 */
	@Override
	public void addCheckpoint(final CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "CompletedCheckpoint");

		final Path path = getPathForCheckpoint(checkpoint.getCheckpointID());
		write(path, checkpoint);
		completedCheckpoints.addLast(checkpoint);

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
			final CompletedCheckpoint completedCheckpoint = completedCheckpoints.removeFirst();
			tryRemoveCompletedCheckpoint(completedCheckpoint, CompletedCheckpoint::discardOnSubsume);
		}

		LOG.info("Added {} to {}.", checkpoint, path);
	}

	private void tryRemoveCompletedCheckpoint(
		CompletedCheckpoint completedCheckpoint,
		ThrowingConsumer<CompletedCheckpoint, Exception> discardCallback) {

		executor.execute(() -> {
			try {
				final Path path = getPathForCheckpoint(completedCheckpoint.getCheckpointID());
				fs.delete(path, false);
				discardCallback.accept(completedCheckpoint);
			} catch (Exception e) {
				LOG.warn("Could not discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), e);
			}
		});
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return new ArrayList<>(completedCheckpoints);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			for (CompletedCheckpoint checkpoint : completedCheckpoints) {
				tryRemoveCompletedCheckpoint(
					checkpoint,
					completedCheckpoint -> completedCheckpoint.discardOnShutdown(jobStatus));
			}

			// delete CHECKPOINTS_PATH
			fs.delete(pathPrefix, true);

			completedCheckpoints.clear();
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			completedCheckpoints.clear();
		}
	}

	// ------------------------------------------------------------------------

	private Path getPathForCheckpoint(long checkpointId) {
		return new Path(pathPrefix, String.format("/%019d", checkpointId));
	}

}
