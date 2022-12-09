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

import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderretrieval.HybridLeaderRetrievalService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.CreateBuilder;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Matchers;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link HybridLeaderElectionService} and the {@link HybridLeaderRetrievalService}.
 */
public class HybridLeaderElectionTest extends TestLogger {

	private static final String LEADER_PATH = "/leader/test-service";

	private TestingServer testingServer;

	private Configuration configuration;

	private FileSystem fs;

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
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		client = ZooKeeperUtils.startCuratorFramework(configuration);

		try {
			tmpDir = Files.createTempDirectory("flink-ha-test-").toFile();
			configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpDir.getAbsolutePath());
			fs = HighAvailabilityServicesUtils.getFileSystem(configuration);
		} catch (IOException e) {
			throw new RuntimeException("Could not init FileSystem.", e);
		}

		executor = Executors.newFixedThreadPool(2, new ExecutorThreadFactory("flink-io"));
	}

	@After
	public void after() throws IOException {
		if (client != null) {
			client.close();
			client = null;
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

	/**
	 * Tests that the HybridLeaderElection/RetrievalService return both the correct URL.
	 */
	@Test
	public void testNormalElectionRetrieval() throws Exception {
		HybridLeaderElectionService leaderElectionService = null;
		HybridLeaderRetrievalService leaderRetrievalService = null;

		try {
			leaderElectionService = new HybridLeaderElectionService(
				client, fs, executor, configuration, LEADER_PATH);
			leaderRetrievalService = new HybridLeaderRetrievalService(
				client, fs, executor, configuration, LEADER_PATH);

			TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
			TestingListener listener = new TestingListener();

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			contender.waitForLeader(timeout);

			assertTrue(contender.isLeader());
			assertEquals(leaderElectionService.getLeaderSessionID(), contender.getLeaderSessionID());

			listener.waitForNewLeader(timeout);

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
	 * Test {@link HybridLeaderElectionService} happen FileSystem error and can be correctly forwarded to the
	 * {@link LeaderContender}.
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

			leaderElectionService = new HybridLeaderElectionService(
				client, fs, executor, configuration, LEADER_PATH);

			testingContender = new TestingContender(TEST_URL, leaderElectionService);

			leaderElectionService.start(testingContender);

			testingContender.waitForError(timeout);

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
	 * Test {@link HybridLeaderElectionService} happen ZooKeeper error and has no impact on
	 * {@link LeaderContender}.
	 */
	@Test
	public void testZooKeeperBasedElectionError() throws Exception {
		HybridLeaderElectionService leaderElectionService = null;
		HybridLeaderRetrievalService leaderRetrievalService = null;
		TestingListener listener = new TestingListener();
		TestingContender testingContender;

		CuratorFramework client;
		final CreateBuilder mockCreateBuilder = mock(CreateBuilder.class, Mockito.RETURNS_DEEP_STUBS);
		final Exception testException = new Exception("Test exception");

		try {
			client = spy(ZooKeeperUtils.startCuratorFramework(configuration));

			doAnswer(invocation -> mockCreateBuilder).when(client).create();

			when(
				mockCreateBuilder
					.creatingParentsIfNeeded()
					.withMode(Matchers.any(CreateMode.class))
					.forPath(anyString(), any(byte[].class))).thenThrow(testException);

			leaderElectionService = new HybridLeaderElectionService(
				client, fs, executor, configuration, LEADER_PATH);
			leaderRetrievalService = new HybridLeaderRetrievalService(
				client, fs, executor, configuration, LEADER_PATH);

			testingContender = new TestingContender(TEST_URL, leaderElectionService);

			leaderElectionService.start(testingContender);
			testingContender.waitForLeader(timeout);

			assertTrue(testingContender.isLeader());
			assertEquals(leaderElectionService.getLeaderSessionID(), testingContender.getLeaderSessionID());

			leaderRetrievalService.start(listener);
			listener.waitForNewLeader(timeout);

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
