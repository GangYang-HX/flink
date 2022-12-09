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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.persistence.AbstractFileSystemReaderWriter;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ArchivedExecutionGraphStore} instances for JobManagers with the FileSystem implement.
 *
 * <p>Each archived execution graph creates a FileSystem path:
 * <pre>
 * +----O /flink/cluster-id/archived-execution-graphs/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/cluster-id/archived-execution-graphs/&lt;job-id&gt; N [persistent]
 * </pre>
 */
public class FileSystemArchivedExecutionGraphStore extends AbstractFileSystemReaderWriter<ArchivedExecutionGraph>
		implements ArchivedExecutionGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemArchivedExecutionGraphStore.class);

	private static final String ARCHIVED_EXECUTION_GRAPHS_PATH = "/archived-execution-graphs";

	private final Path pathPrefix;

	/**
	 * The map of all current exist ArchivedExecutionGraphs in FileSystem.
	 */
	private final Map<JobID, ArchivedExecutionGraph> archivedExecutionGraphsCache;

	public FileSystemArchivedExecutionGraphStore(FileSystem fs, Configuration configuration) {
		super(fs, configuration);

		this.pathPrefix = new Path(
			HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration),
			ARCHIVED_EXECUTION_GRAPHS_PATH);

		archivedExecutionGraphsCache = recoverArchivedExecutionGraphs();
	}

    private Map<JobID, ArchivedExecutionGraph> recoverArchivedExecutionGraphs() {
        try {
			if (!fs.exists(pathPrefix) || !fs.getFileStatus(pathPrefix).isDir()) {
				return new HashMap<>();
			}

            return Arrays.stream(fs.listStatus(pathPrefix))
                    .collect(Collectors.toMap(
						s -> FileSystemJobGraphStoreUtil.INSTANCE.nameToJobID(s.getPath().getName()),
						s -> {
							try {
								return read(s.getPath());
							} catch (IOException e) {
								LOG.error("recover ArchivedExecutionGraphs from FileSystem error", e);
								return null;
							}}
					));

        } catch (IOException e) {
			LOG.error("recover ArchivedExecutionGraphs from FileSystem error", e);
			return new HashMap<>();
        }
	}

	@Override
	public int size() {
        return archivedExecutionGraphsCache.size();
	}

	@Nullable
	@Override
	public ArchivedExecutionGraph get(JobID jobId) {
		checkNotNull(jobId, "Job ID");

        return archivedExecutionGraphsCache.get(jobId);
	}

	@Override
	public void put(ArchivedExecutionGraph archivedExecutionGraph) throws IOException {
		checkNotNull(archivedExecutionGraph, "ArchivedExecutionGraph");

		Path path = archivedExecutionGraphPath(archivedExecutionGraph.getJobID());

		try {
			write(path, archivedExecutionGraph);
			archivedExecutionGraphsCache.put(archivedExecutionGraph.getJobID(), archivedExecutionGraph);
			LOG.info("put ArchivedExecutionGraph to {} success", path);
		} catch (IOException e) {
			LOG.error("put ArchivedExecutionGraph to {} error", path, e);
		}
	}

	@Override
	public JobsOverview getStoredJobsOverview() {
		Collection<JobStatus> allJobStatus = archivedExecutionGraphsCache.values().stream()
			.map(ArchivedExecutionGraph::getState)
			.collect(Collectors.toList());

		return JobsOverview.create(allJobStatus);
	}

	@Override
	public Collection<JobDetails> getAvailableJobDetails() {
		return archivedExecutionGraphsCache.values().stream()
			.map(WebMonitorUtils::createDetailsForJob)
			.collect(Collectors.toList());
	}

	@Nullable
	@Override
	public JobDetails getAvailableJobDetails(JobID jobId) {
		final ArchivedExecutionGraph archivedExecutionGraph = archivedExecutionGraphsCache.get(jobId);

		if (archivedExecutionGraph != null) {
			return WebMonitorUtils.createDetailsForJob(archivedExecutionGraph);
		} else {
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		// don't implement because we don't need to close FileSystem here
	}

	@Override
	public void remove(JobID jobId) throws IOException {
		checkNotNull(jobId, "Job ID");

		Path path = archivedExecutionGraphPath(jobId);
		fs.delete(path, false);
		archivedExecutionGraphsCache.remove(jobId);

		LOG.info("remove ArchivedExecutionGraph from {}.", path);
	}

	private Path archivedExecutionGraphPath(JobID jobID) {
		return new Path(pathPrefix, FileSystemJobGraphStoreUtil.INSTANCE.jobIDToName(jobID));
	}
}
