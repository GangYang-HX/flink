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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for the {@link FileSystemArchivedExecutionGraphStore}.
 */
public class FileSystemArchivedExecutionGraphStoreTest extends TestLogger {

	private Configuration configuration;

	private FileSystem fs;

	private File tmpDir;

	@Before
	public void before() throws Exception {
		configuration = new Configuration();

		tmpDir = Files.createTempDirectory("flink-ha-test-").toFile();
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpDir.getAbsolutePath());

		fs = HighAvailabilityServicesUtils.getFileSystem(configuration);
	}

	@After
	public void after() throws Exception {
		if (tmpDir != null) {
			FileUtils.deleteDirectory(tmpDir);
			tmpDir = null;
		}
	}

    /**
     * Tests that we can put {@link ArchivedExecutionGraph} into the
     * {@link FileSystemArchivedExecutionGraphStoreTest} and that the graph is persisted.
     */
    @Test
    public void testPutAndGet() throws IOException {
        final ArchivedExecutionGraph mockExecutionGraph = new ArchivedExecutionGraphBuilder().setState(JobStatus.RUNNING).build();
        final JobID jobID = mockExecutionGraph.getJobID();

		final FileSystemArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore();

		// check that the storage directory is empty
		assertEquals(0, executionGraphStore.size());

		// check put and get
		executionGraphStore.put(mockExecutionGraph);
		assertEquals(1, executionGraphStore.size());
		assertEquals(mockExecutionGraph, executionGraphStore.get(jobID));

		// check that we have persisted the given execution graph
		final FileSystemArchivedExecutionGraphStore restoreExecutionGraphStore = createDefaultExecutionGraphStore();
		assertEquals(1, restoreExecutionGraphStore.size());
		assertEquals(executionGraphStore.getAvailableJobDetails(jobID), restoreExecutionGraphStore.getAvailableJobDetails(jobID));
    }

    @Test
    public void testRemove() throws IOException {
		final ArchivedExecutionGraph executionGraph1 = new ArchivedExecutionGraphBuilder().setState(JobStatus.RUNNING).build();
		final ArchivedExecutionGraph executionGraph2 = new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();

		// first put
		final FileSystemArchivedExecutionGraphStore executionGraphStore = createDefaultExecutionGraphStore();
		assertEquals(0, executionGraphStore.size());
		executionGraphStore.put(executionGraph1);
		executionGraphStore.put(executionGraph2);

		// remove exist jobId
		final FileSystemArchivedExecutionGraphStore restoreExecutionGraphStore = createDefaultExecutionGraphStore();
		assertEquals(2, restoreExecutionGraphStore.size());
		restoreExecutionGraphStore.remove(executionGraph1.getJobID());

		// remove not exist jobId
		restoreExecutionGraphStore.remove(new JobID());

		// check remove success
		final FileSystemArchivedExecutionGraphStore removedExecutionGraphStore = createDefaultExecutionGraphStore();
		assertEquals(1, removedExecutionGraphStore.size());
		assertNull(removedExecutionGraphStore.get(executionGraph1.getJobID()));
		assertNotNull(removedExecutionGraphStore.get(executionGraph2.getJobID()));
	}

    private FileSystemArchivedExecutionGraphStore createDefaultExecutionGraphStore() {
        return new FileSystemArchivedExecutionGraphStore(fs, configuration);
    }

}
