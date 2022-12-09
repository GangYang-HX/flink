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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus;
import org.apache.flink.runtime.highavailability.hybrid.FileSystemRunningJobRegistry;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;


public class FileSystemRegistryTest extends TestLogger {

	/**
	 * Tests that the function of FileSystemRunningJobRegistry, setJobRunning(), setJobFinished(), isJobRunning()
	 */
	@Test
	public void testZooKeeperRegistry() throws Exception {
		Configuration configuration = new Configuration();

		File tmpDir = Files.createTempDirectory("flink-ha-test-").toFile();
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpDir.getAbsolutePath());

		FileSystem fs = HighAvailabilityServicesUtils.getFileSystem(configuration);

		final RunningJobsRegistry registry = new FileSystemRunningJobRegistry(fs, configuration);

		JobID jobID = JobID.generate();
		assertEquals(JobSchedulingStatus.PENDING, registry.getJobSchedulingStatus(jobID));

		registry.setJobRunning(jobID);
		assertEquals(JobSchedulingStatus.RUNNING, registry.getJobSchedulingStatus(jobID));

		registry.setJobFinished(jobID);
		assertEquals(JobSchedulingStatus.DONE, registry.getJobSchedulingStatus(jobID));

		registry.clearJob(jobID);
		assertEquals(JobSchedulingStatus.PENDING, registry.getJobSchedulingStatus(jobID));

		FileUtils.deleteDirectory(tmpDir);
	}
}
