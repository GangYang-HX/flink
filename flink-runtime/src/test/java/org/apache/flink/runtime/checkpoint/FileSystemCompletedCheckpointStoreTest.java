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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.persistence.filesystem.FileSystemReaderWriterHelper;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

/** Tests for basic {@link CompletedCheckpointStore} contract and FileSystem state handling. */
public class FileSystemCompletedCheckpointStoreTest extends CompletedCheckpointStoreTest {

    private static final String CHECKPOINT_PATH = "/checkpoints";

    private FileSystem fs;

    private Configuration configuration;

    private File tmpDir;

    private Path prefixPath;

    @Before
    public void before() {
        configuration = new Configuration();

        try {
            tmpDir = Files.createTempDirectory("flink-ha-test-").toFile();
            configuration.setString(
                    HighAvailabilityOptions.HA_STORAGE_PATH, tmpDir.getAbsolutePath());
            fs = HighAvailabilityServicesUtils.getFileSystem(configuration);
            prefixPath =
                    new Path(
                            new Path(
                                    HighAvailabilityServicesUtils
                                            .getClusterHighAvailableStoragePath(configuration),
                                    CHECKPOINT_PATH),
                            HighAvailabilityServices.DEFAULT_JOB_ID.toString());
        } catch (IOException e) {
            throw new RuntimeException("Could not init FileSystem.", e);
        }
    }

    @After
    public void after() throws Exception {
        testDiscardAllCheckpoints();

        if (tmpDir != null) {
            FileUtils.deleteDirectory(tmpDir);
            tmpDir = null;
        }
    }

    @Override
    protected CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain, Executor executor) throws Exception {
        FileSystemReaderWriterHelper<CompletedCheckpoint> readerWriterHelper =
                new FileSystemReaderWriterHelper(fs, configuration);

        Collection<CompletedCheckpoint> completedCheckpoints =
                FileSystemCompletedCheckpointStoreUtils.retrieveCompletedCheckpoint(
                        fs, readerWriterHelper, prefixPath);

        return new FileSystemCompletedCheckpointStore(
                fs,
                readerWriterHelper,
                prefixPath,
                maxNumberOfCheckpointsToRetain,
                completedCheckpoints,
                SharedStateRegistry.DEFAULT_FACTORY.create(
                        org.apache.flink.util.concurrent.Executors.directExecutor(),
                        emptyList(),
                        RestoreMode.LEGACY),
                executor);
    }

    /**
     * Tests that older checkpoints are not cleaned up right away when recovering. Only after
     * another checkpointed has been completed the old checkpoints exceeding the number of
     * checkpoints to retain will be removed.
     */
    @Test
    public void testRecovery() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CompletedCheckpointStore checkpoints = createRecoveredCompletedCheckpointStore(3);

        TestCompletedCheckpoint[] expected =
                new TestCompletedCheckpoint[] {
                    createCheckpoint(0, sharedStateRegistry),
                    createCheckpoint(1, sharedStateRegistry),
                    createCheckpoint(2, sharedStateRegistry)
                };

        // Add multiple checkpoints
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[0], new CheckpointsCleaner(), () -> {});
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[1], new CheckpointsCleaner(), () -> {});
        checkpoints.addCheckpointAndSubsumeOldestOne(
                expected[2], new CheckpointsCleaner(), () -> {});

        verifyCheckpointRegistered(expected[0].getOperatorStates().values(), sharedStateRegistry);
        verifyCheckpointRegistered(expected[1].getOperatorStates().values(), sharedStateRegistry);
        verifyCheckpointRegistered(expected[2].getOperatorStates().values(), sharedStateRegistry);

        assertEquals(3, fs.listStatus(prefixPath).length);
        assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());

        // Recover
        sharedStateRegistry.close();
        sharedStateRegistry = new SharedStateRegistryImpl();

        assertEquals(3, fs.listStatus(prefixPath).length);
        assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());
        assertEquals(expected[2], checkpoints.getLatestCheckpoint());

        List<CompletedCheckpoint> expectedCheckpoints = new ArrayList<>(3);
        expectedCheckpoints.add(expected[1]);
        expectedCheckpoints.add(expected[2]);
        expectedCheckpoints.add(createCheckpoint(3, sharedStateRegistry));

        checkpoints.addCheckpointAndSubsumeOldestOne(
                expectedCheckpoints.get(2), new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> actualCheckpoints = checkpoints.getAllCheckpoints();

        assertEquals(expectedCheckpoints, actualCheckpoints);

        for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
            verifyCheckpointRegistered(
                    actualCheckpoint.getOperatorStates().values(), sharedStateRegistry);
        }
    }
}
