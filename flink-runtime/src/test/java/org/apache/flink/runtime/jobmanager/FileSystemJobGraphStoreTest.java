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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests for the {@link FileSystemJobGraphStore}. */
public class FileSystemJobGraphStoreTest extends TestLogger {

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

    @Test
    public void testPutAndRemoveJobGraph() throws Exception {
        JobGraphStore jobGraphs = createJobGraphStore();

        try {
            JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);
            jobGraphs.start(listener);

            JobGraph jobGraph = createJobGraph(new JobID(), "JobName");

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());

            // Add initial
            jobGraphs.putJobGraph(jobGraph);

            // Verify initial job graph
            Collection<JobID> jobIds = jobGraphs.getJobIds();
            assertEquals(1, jobIds.size());

            JobID jobId = jobIds.iterator().next();

            verifyJobGraphs(jobGraph, jobGraphs.recoverJobGraph(jobId));

            // Update (same ID)
            jobGraph = createJobGraph(jobGraph.getJobID(), "Updated JobName");
            jobGraphs.putJobGraph(jobGraph);

            // Verify updated
            jobIds = jobGraphs.getJobIds();
            assertEquals(1, jobIds.size());

            jobId = jobIds.iterator().next();

            verifyJobGraphs(jobGraph, jobGraphs.recoverJobGraph(jobId));

            // Remove
            jobGraphs.globalCleanupAsync(jobGraph.getJobID(), Executors.directExecutor()).join();

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());

            // Nothing should have been notified
            verify(listener, atMost(1)).onAddedJobGraph(any(JobID.class));
            verify(listener, never()).onRemovedJobGraph(any(JobID.class));

            // Don't fail if called again
            jobGraphs.globalCleanupAsync(jobGraph.getJobID(), Executors.directExecutor()).join();
        } finally {
            jobGraphs.stop();
        }
    }

    @Test
    public void testRecoverJobGraphs() throws Exception {
        JobGraphStore jobGraphs = createJobGraphStore();

        try {
            JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

            jobGraphs.start(listener);

            HashMap<JobID, JobGraph> expected = new HashMap<>();
            JobID[] jobIds = new JobID[] {new JobID(), new JobID(), new JobID()};

            expected.put(jobIds[0], createJobGraph(jobIds[0]));
            expected.put(jobIds[1], createJobGraph(jobIds[1]));
            expected.put(jobIds[2], createJobGraph(jobIds[2]));

            // Add all
            for (JobGraph jobGraph : expected.values()) {
                jobGraphs.putJobGraph(jobGraph);
            }

            Collection<JobID> actual = jobGraphs.getJobIds();

            assertEquals(expected.size(), actual.size());

            for (JobID jobId : actual) {
                JobGraph jobGraph = jobGraphs.recoverJobGraph(jobId);
                assertTrue(expected.containsKey(jobGraph.getJobID()));

                verifyJobGraphs(expected.get(jobGraph.getJobID()), jobGraph);

                jobGraphs
                        .globalCleanupAsync(jobGraph.getJobID(), Executors.directExecutor())
                        .join();
            }

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());

            // Nothing should have been notified
            verify(listener, atMost(expected.size())).onAddedJobGraph(any(JobID.class));
            verify(listener, never()).onRemovedJobGraph(any(JobID.class));
        } finally {
            jobGraphs.stop();
        }
    }

    @Test
    public void testRemoveAllJobGraphs() throws Exception {
        JobGraphStore jobGraphs = createJobGraphStore();

        try {
            jobGraphs.start(null);

            JobID[] jobIds = new JobID[] {new JobID(), new JobID(), new JobID()};
            jobGraphs.putJobGraph(createJobGraph(jobIds[0]));
            jobGraphs.putJobGraph(createJobGraph(jobIds[1]));
            jobGraphs.putJobGraph(createJobGraph(jobIds[2]));

            Collection<JobID> actual = jobGraphs.getJobIds();

            assertEquals(jobIds.length, actual.size());

            for (JobID jobId : actual) {
                jobGraphs.globalCleanupAsync(jobId, Executors.directExecutor()).join();
            }

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());
        } finally {
            jobGraphs.stop();
        }
    }

    private JobGraphStore createJobGraphStore() {
        return new FileSystemJobGraphStore(fs, configuration);
    }

    private JobGraph createJobGraph(JobID jobId) {
        return createJobGraph(jobId, "Test JobGraph");
    }

    private JobGraph createJobGraph(JobID jobId, String jobName) {
        final JobGraph jobGraph = new JobGraph(jobId, jobName);

        final JobVertex jobVertex = new JobVertex("Test JobVertex");
        jobVertex.setParallelism(1);

        jobGraph.addVertex(jobVertex);

        return jobGraph;
    }

    private void verifyJobGraphs(JobGraph expected, JobGraph actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getJobID(), actual.getJobID());
    }
}
