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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.persistence.AbstractFileSystemReaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link JobGraph} instances for JobManagers with the FileSystem implement.
 *
 * <p>Each job graph creates a FileSystem path:
 * <pre>
 * +----O /flink/cluster-id/jobgraphs/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/cluster-id/jobgraphs/&lt;job-id&gt; N [persistent]
 * </pre>
 */
public class FileSystemJobGraphStore extends AbstractFileSystemReaderWriter<JobGraph>
	implements JobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJobGraphStore.class);

	private static final String JOB_GRAPHS_PATH = "/jobgraphs";

	private final Path pathPrefix;

	/**
	 * The set of IDs of all added job graphs.
	 */
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/**
	 * The external listener to be notified on races.
	 */
	private JobGraphListener jobGraphListener;

	/**
	 * Flag indicating whether this instance is running.
	 */
	private boolean isRunning;

	public FileSystemJobGraphStore(FileSystem fs, Configuration configuration) {
		super(fs, configuration);

		this.pathPrefix = new Path(
			HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration),
			JOB_GRAPHS_PATH);
	}

	@Override
	public void start(JobGraphListener jobGraphListener) throws Exception {
		if (!isRunning) {
			this.jobGraphListener = jobGraphListener;
			isRunning = true;
		}
	}

	@Override
	public void stop() throws Exception {
		if (isRunning) {
			jobGraphListener = null;
			isRunning = false;
		}
	}

	@Override
	@Nullable
	public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");

		Path path = getPathForJobGraph(jobId);

		verifyIsRunning();
		JobGraph jobGraph = read(path);
		addedJobGraphs.add(jobGraph.getJobID());

		LOG.info("Recovering job graph {} from {}.", jobId, path);
		return jobGraph;
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		Path path = getPathForJobGraph(jobGraph.getJobID());

		write(path, jobGraph);
		addedJobGraphs.add(jobGraph.getJobID());
		LOG.info("Added {} to FileSystem.", jobGraph);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		Path path = getPathForJobGraph(jobId);

		fs.delete(path, false);
		addedJobGraphs.remove(jobId);
		LOG.info("Removing job graph {} from {}.", jobId, path);
	}

	@Override
	public void releaseJobGraph(JobID jobId) {
		addedJobGraphs.remove(jobId);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		LOG.info("Retrieving all stored job ids from file system under {}.", pathPrefix);

		if (!fs.exists(pathPrefix) || !fs.getFileStatus(pathPrefix).isDir()) {
			return Collections.emptyList();
		}

		// First get all checkpoint paths from file system
		FileStatus[] statuses = fs.listStatus(pathPrefix);
		Collection<JobID> jobIds = Arrays.stream(statuses)
			.map(s -> FileSystemJobGraphStoreUtil.INSTANCE.nameToJobID(s.getPath().getName()))
			.collect(Collectors.toList());

		LOG.info("Found {} JobId in file system.", jobIds.size());

		return jobIds;
	}

	/**
	 * Verifies that the state is running.
	 */
	private void verifyIsRunning() {
		checkState(isRunning, "Not running. Forgot to call start()?");
	}

	private Path getPathForJobGraph(JobID jobID) {
		return new Path(pathPrefix, FileSystemJobGraphStoreUtil.INSTANCE.jobIDToName(jobID));
	}
}
