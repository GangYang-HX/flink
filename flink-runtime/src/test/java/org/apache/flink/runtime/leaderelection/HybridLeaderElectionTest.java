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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.leaderretrieval.HybridLeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link HybridLeaderElectionService} and the {@link HybridLeaderRetrievalService}.
 */
public class HybridLeaderElectionTest extends TestLogger {

    private static final String LEADER_PATH = "/leader/test-service";

    private TestingServer testingServer;

    private Configuration configuration;

    private FileSystem fs;

    private CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private CuratorFramework client;

    private Executor executor;

    private File tmpDir;

    private static final String TEST_URL = "akka://user/jobmanager";

    private static final long timeout = 200L * 1000L;

    @Before
    public void before() {
        try {
            testingServer = new TestingServer();
        } catch (Exception e) {
            throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
        }

        configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
        configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

        curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration,
                        error ->
                                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), error));
        client = curatorFrameworkWrapper.asCuratorFramework();

        try {
            tmpDir = Files.createTempDirectory("flink-ha-test-").toFile();
            configuration.setString(
                    HighAvailabilityOptions.HA_STORAGE_PATH, tmpDir.getAbsolutePath());
            fs = HighAvailabilityServicesUtils.getFileSystem(configuration);
        } catch (IOException e) {
            throw new RuntimeException("Could not init FileSystem.", e);
        }

        executor = Executors.newFixedThreadPool(2, new ExecutorThreadFactory("flink-io"));
    }

    @After
    public void after() throws IOException {
        if (curatorFrameworkWrapper != null) {
            curatorFrameworkWrapper.close();
            curatorFrameworkWrapper = null;
        }

        if (testingServer != null) {
            testingServer.stop();
            testingServer = null;
        }

        if (tmpDir != null) {
            FileUtils.deleteDirectory(tmpDir);
            tmpDir = null;
        }
    }

    /** Tests that the HybridLeaderElection/RetrievalService return both the correct URL. */
    @Test
    public void testNormalElectionRetrieval() throws Exception {
        HybridLeaderElectionService leaderElectionService = null;
        HybridLeaderRetrievalService leaderRetrievalService = null;

        try {
            leaderElectionService =
                    new HybridLeaderElectionService(
                            client, fs, executor, configuration, LEADER_PATH);
            leaderRetrievalService =
                    new HybridLeaderRetrievalService(
                            client, fs, executor, configuration, LEADER_PATH);

            TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
            TestingListener listener = new TestingListener();

            leaderElectionService.start(contender);
            leaderRetrievalService.start(listener);

            contender.waitForLeader();

            assertTrue(contender.isLeader());
            assertEquals(
                    leaderElectionService.getLeaderSessionID(), contender.getLeaderSessionID());

            listener.waitForNewLeader();

            assertEquals(TEST_URL, listener.getAddress());
            assertEquals(leaderElectionService.getLeaderSessionID(), listener.getLeaderSessionID());

        } finally {
            if (leaderElectionService != null) {
                leaderElectionService.stop();
            }

            if (leaderRetrievalService != null) {
                leaderRetrievalService.stop();
            }
        }
    }

    /**
     * Test {@link HybridLeaderElectionService} happen FileSystem error and can be correctly
     * forwarded to the {@link LeaderContender}.
     */
    @Test
    public void testFileSystemBasedElectionError() throws Exception {
        HybridLeaderElectionService leaderElectionService = null;
        TestingContender testingContender;

        final FileSystem fs = mock(FileSystem.class, Mockito.RETURNS_DEEP_STUBS);
        final IOException testException = new IOException("Test exception");

        try {
            when(fs.create(any(Path.class), any(FileSystem.WriteMode.class)))
                    .thenThrow(testException);

            leaderElectionService =
                    new HybridLeaderElectionService(
                            client, fs, executor, configuration, LEADER_PATH);

            testingContender = new TestingContender(TEST_URL, leaderElectionService);

            leaderElectionService.start(testingContender);

            testingContender.waitForError();

            assertNotNull(testingContender.getError());
            assertEquals(testException, testingContender.getError().getCause());
        } finally {
            if (leaderElectionService != null) {
                try {
                    leaderElectionService.stop();
                } catch (Exception e) {
                    // do nothing
                }
            }
        }
    }

    /**
     * Test {@link HybridLeaderElectionService} happen ZooKeeper error and has no impact on {@link
     * LeaderContender}.
     */
    @Test
    public void testZooKeeperBasedElectionError() throws Exception {
        HybridLeaderElectionService leaderElectionService = null;
        HybridLeaderRetrievalService leaderRetrievalService = null;

        configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, "localhost:1111");
        configuration.setInteger(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS, 0);
        configuration.setInteger(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT, 2000);

        TestingListener listener = new TestingListener();
        TestingContender testingContender;

        try {
            leaderElectionService =
                    new HybridLeaderElectionService(
                            client, fs, executor, configuration, LEADER_PATH);
            leaderRetrievalService =
                    new HybridLeaderRetrievalService(
                            client, fs, executor, configuration, LEADER_PATH);

            testingContender = new TestingContender(TEST_URL, leaderElectionService);

            leaderElectionService.start(testingContender);
            testingContender.waitForLeader();

            assertTrue(testingContender.isLeader());
            assertEquals(
                    leaderElectionService.getLeaderSessionID(),
                    testingContender.getLeaderSessionID());

            leaderRetrievalService.start(listener);
            listener.waitForNewLeader();

            assertEquals(TEST_URL, listener.getAddress());
            assertEquals(leaderElectionService.getLeaderSessionID(), listener.getLeaderSessionID());
        } finally {
            if (leaderElectionService != null) {
                leaderElectionService.stop();
            }

            if (leaderRetrievalService != null) {
                leaderRetrievalService.stop();
            }
        }
    }
}
