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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * Tests for basic {@link CompletedCheckpointStore} contract and FileSystem state handling.
 */
public class FileSystemCompletedCheckpointStoreTest extends CompletedCheckpointStoreTest {

	private FileSystem fs;

	private Executor executor;

	private Configuration configuration;

	private File tmpDir;

	@Before
	public void before() {
		configuration = new Configuration();

		try {
			tmpDir = Files.createTempDirectory("flink-ha-test-").toFile();
			configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpDir.getAbsolutePath());
			fs = HighAvailabilityServicesUtils.getFileSystem(configuration);
		} catch (IOException e) {
			throw new RuntimeException("Could not init FileSystem.", e);
		}

		executor = Executors.newFixedThreadPool(5);
	}

	@After
	public void after() throws Exception {
		testDiscardAllCheckpoints();

		if (tmpDir != null) {
			FileUtils.deleteDirectory(tmpDir);
			tmpDir = null;
		}
	}

	/**
	 * Tests that older checkpoints are not cleaned up right away when recovering. Only after
	 * another checkpointed has been completed the old checkpoints exceeding the number of
	 * checkpoints to retain will be removed.
	 */
	@Test
	public void testRecovery() throws Exception {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(3);

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[]{
			createCheckpoint(0, sharedStateRegistry),
			createCheckpoint(1, sharedStateRegistry),
			createCheckpoint(2, sharedStateRegistry)
		};

		// Add multiple checkpoints
		checkpoints.addCheckpoint(expected[0]);
		checkpoints.addCheckpoint(expected[1]);
		checkpoints.addCheckpoint(expected[2]);

		verifyCheckpointRegistered(expected[0].getOperatorStates().values(), sharedStateRegistry);
		verifyCheckpointRegistered(expected[1].getOperatorStates().values(), sharedStateRegistry);
		verifyCheckpointRegistered(expected[2].getOperatorStates().values(), sharedStateRegistry);

		assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());

		List<CompletedCheckpoint> expectedCheckpoints = checkpoints.getAllCheckpoints();

		// Recover
		sharedStateRegistry.close();
		sharedStateRegistry = new SharedStateRegistry();

		checkpoints = createCompletedCheckpoints(3);
		checkpoints.recover();

		assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());

		List<CompletedCheckpoint> actualCheckpoints = checkpoints.getAllCheckpoints();

		assertEquals(expectedCheckpoints, actualCheckpoints);

		for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
			verifyCheckpointRegistered(actualCheckpoint.getOperatorStates().values(), sharedStateRegistry);
		}
	}

	@Override
	protected CompletedCheckpointStore createCompletedCheckpoints(int maxNumberOfCheckpointsToRetain) throws Exception {
		configuration.setInteger(
			CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, maxNumberOfCheckpointsToRetain);

		return new FileSystemCompletedCheckpointStore(
			fs, configuration, HighAvailabilityServices.DEFAULT_JOB_ID, executor);
	}
}
