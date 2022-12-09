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

package org.apache.flink.runtime.highavailability.hybrid;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.FileSystemCheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.FileSystemArchivedExecutionGraphStore;
import org.apache.flink.runtime.jobmanager.FileSystemJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.HybridLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.HybridLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link HighAvailabilityServices} using Apache ZooKeeper and an external FileSystem.
 * <p>This implement should run on Yarn which only exists one running JobManager for one cluster, so we don't
 * consider concurrent race problem.
 * <p>The services store data in ZooKeeper's nodes or FileSystem's path, there exists two type data:
 *
 * <ul>1. leader information: save to FileSystem is a necessary and try best to save to ZooKeeper at the same time.</ul>
 * <ul>2. Checkpoint, JobGraph, RunningJobsRegistry: only save to FileSystem.</ul>
 *
 * <p>The store node or path's tree structure is as follows:
 * <ul>ZooKeeper
 * <pre>
 * /flink
 *      +/cluster_id_1/leader/resource_manager
 *      |                    +/dispatcher
 *      |                    +/rest_server
 *      |                    +/job_manager/job-id-1
 *      |                    +/job_manager/job-id-2
 *      +/cluster_id_2/leader/resource_manager
 *      |                    +/dispatcher
 *      |                    +/rest_server
 *      |                    +/job_manager/job-id-1
 * </pre>
 *
 * <ul>FileSystem
 * <pre>
 * /flink
 *      +/cluster_id_1/leader/resource_manager
 *      |                    +/dispatcher
 *      |                    +/rest_server
 *      |                    +/job_manager/job-id-1
 *      |                    +/job_manager/job-id-2
 *      |             +/checkpoints/job-id-1/ck-id-1
 *      |                                   +/ck-id-2
 *      |                          +/job-id-2/ck-id-1
 *      |             +/jobgraphs/job-id-1
 *      |                        +/job-id-2
 *      |             +/running_job_registry/job-id-1
 *      |                                   +/job-id-2
 *      |             +/checkpoint_id_counter/job-id-1
 *      |                                   +/job-id-2
 *      +/cluster_id_2/leader/resource_manager
 *      |                    +/dispatcher
 *      |                    +/rest_server
 *      |                    +/job_manager/job-id-1
 *      |             +/checkpoints/job-id-1/ck-id-1
 *      |                                   +/ck-id-2
 *      |                          +/job-id-2/ck-id-1
 *      |             +/jobgraphs/job-id-1
 *      |                        +/job-id-2
 *      |             +/running_job_registry/job-id-1
 *      |                                   +/job-id-2
 *      |             +/checkpoint_id_counter/job-id-1
 *      |                                   +/job-id-2
 * </pre>
 *
 * <p>The root path "/flink" is configurable via the option
 * {@link HighAvailabilityOptions#HA_ZOOKEEPER_ROOT} or {@link HighAvailabilityOptions#HA_STORAGE_PATH}.
 * This makes sure Flink stores its data under specific subtrees, for example to accommodate specific permission.
 *
 * <p>The "cluster_id" part identifies the data stored for a specific Flink "cluster".
 * This "cluster" can be either a standalone or containerized Flink cluster, or it can be job
 * on a framework like YARN or Mesos (in a "per-job-cluster" mode).
 *
 * <p>In case of a "per-job-cluster" on YARN or Mesos, the cluster-id is generated and configured
 * automatically by the client or dispatcher that submits the Job to YARN or Mesos.
 */
public class HybridHaServices implements HighAvailabilityServices {

	private static final Logger LOG = LoggerFactory.getLogger(HybridHaServices.class);

	private static final String RESOURCE_MANAGER_LEADER_PATH = "/leader/resource_manager";

	private static final String DISPATCHER_LEADER_PATH = "/leader/dispatcher";

	private static final String JOB_MANAGER_LEADER_PATH = "/leader/job_manager/%s";

	private static final String REST_SERVER_LEADER_PATH = "/leader/rest_server";

	/** The ZooKeeper client to use. */
	private final CuratorFramework client;

	/** The FileSystem to use. */
	private final FileSystem fs;

	/** The executor to run time-consuming callbacks. */
	private final Executor executor;

	/** The runtime configuration. */
	private final Configuration configuration;

	/** Store for arbitrary blobs. */
	private final BlobStoreService blobStoreService;

	public HybridHaServices(
		CuratorFramework client,
		FileSystem fs,
		Executor executor,
		Configuration configuration,
		BlobStoreService blobStoreService
	) {
		this.client = checkNotNull(client, "CuratorFramework Client");
		this.fs = checkNotNull(fs, "FileSystem");
		this.executor = checkNotNull(executor, "Executor");
		this.configuration = checkNotNull(configuration, "Configuration");
		this.blobStoreService = checkNotNull(blobStoreService);
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return new HybridLeaderRetrievalService(client, fs, executor, configuration, RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return new HybridLeaderRetrievalService(client, fs, executor, configuration, DISPATCHER_LEADER_PATH);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return new HybridLeaderRetrievalService(
			client, fs, executor, configuration, String.format(JOB_MANAGER_LEADER_PATH, jobID));
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		return new HybridLeaderRetrievalService(client, fs, executor, configuration, REST_SERVER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return new HybridLeaderElectionService(client, fs, executor, configuration, RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return new HybridLeaderElectionService(client, fs, executor, configuration, DISPATCHER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		return new HybridLeaderElectionService(
			client, fs, executor, configuration, String.format(JOB_MANAGER_LEADER_PATH, jobID));
	}

	@Override
	public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
		return new HybridLeaderElectionService(client, fs, executor, configuration, REST_SERVER_LEADER_PATH);
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new FileSystemCheckpointRecoveryFactory(fs, configuration, executor);
	}

	@Override
	public JobGraphStore getJobGraphStore() throws Exception {
		return new FileSystemJobGraphStore(fs, configuration);
	}

	@Override
	public ArchivedExecutionGraphStore getArchivedExecutionGraphStore() throws Exception {
		return new FileSystemArchivedExecutionGraphStore(fs, configuration);
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		return new FileSystemRunningJobRegistry(fs, configuration);
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return blobStoreService;
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		Throwable exception = null;

		try {
			blobStoreService.close();
		} catch (Throwable t) {
			exception = t;
		}

		internalClose();

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close the HybridHaServices.");
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		LOG.info("Close and clean up all data for HybridHaServices.");

		Throwable exception = null;

		try {
			blobStoreService.closeAndCleanupAllData();
		} catch (Throwable t) {
			exception = t;
		}

		try {
			ZooKeeperUtils.cleanupZooKeeperPaths(client, configuration);
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		internalClose();

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close and clean up all data of HybridHaServices.");
		}
	}

	/**
	 * Closes components which don't distinguish between close and closeAndCleanupAllData.
	 */
	private void internalClose() {
		client.close();
	}

}
