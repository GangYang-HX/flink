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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionStateListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * The counterpart to the {@link
 * org.apache.flink.runtime.leaderelection.HybridLeaderElectionService}.
 *
 * <p>Normally, the leader address as well as the current leader session ID is retrieved from
 * ZooKeeper. If ZooKeeper catches some error, it will be retrieved from the external FileSystem.
 */
public class HybridLeaderRetrievalService
        implements LeaderRetrievalService, NodeCacheListener, UnhandledErrorListener {
    private static final Logger LOG = LoggerFactory.getLogger(HybridLeaderRetrievalService.class);

    private final Object lock = new Object();

    /** Connection to the used ZooKeeper quorum. */
    private final CuratorFramework client;

    private final FileSystem fs;

    private final Configuration configuration;

    /** Curator recipe to watch changes of a specific ZooKeeper node. */
    private final NodeCache cache;

    /** retrieval path suffix of the current leader information. */
    private final String pathSuffix;

    /** FileSystem polling interval. */
    private final int pollingInterval;

    /** Listener which will be notified about leader changes. */
    private volatile LeaderRetrievalListener leaderListener;

    private LeaderInformation lastLeaderInformation;

    private volatile boolean running;

    /** Is ZooKeeper available. */
    private volatile boolean zkAvailable = true;

    /** The number of switching to FileSystem Retrieval. */
    private volatile int switchFileSystemNum;

    /** Executor to run FileSystem retrieval. */
    private final Executor executor;

    private final ConnectionStateListener connectionStateListener =
            (client, newState) -> handleStateChange(newState);

    public HybridLeaderRetrievalService(
            CuratorFramework client,
            FileSystem fs,
            Executor executor,
            Configuration configuration,
            String pathSuffix) {
        this.client = Preconditions.checkNotNull(client, "CuratorFramework client");
        this.fs = Preconditions.checkNotNull(fs, "FileSystem");
        this.executor = Preconditions.checkNotNull(executor, "Executor");
        this.configuration = Preconditions.checkNotNull(configuration, "Configuration");
        this.pathSuffix = Preconditions.checkNotNull(pathSuffix, "pathSuffix");
        this.cache = new NodeCache(client, pathSuffix);
        this.pollingInterval =
                configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_RETRIEVAL_INTERVAL);
    }

    @Override
    public void start(LeaderRetrievalListener listener) throws Exception {
        Preconditions.checkNotNull(listener, "Listener must not be null.");
        Preconditions.checkState(
                leaderListener == null, "HybridLeaderRetrievalService can only be started once.");

        running = true;

        leaderListener = listener;
        client.getUnhandledErrorListenable().addListener(this);
        cache.getListenable().addListener(this);
        cache.start();
        client.getConnectionStateListenable().addListener(connectionStateListener);

        LOG.info("Starting HybridLeaderRetrievalService, listener={}.", listener);
    }

    @Override
    public void stop() throws Exception {
        synchronized (lock) {
            if (!running) {
                return;
            }

            running = false;
        }

        client.getUnhandledErrorListenable().removeListener(this);
        client.getConnectionStateListenable().removeListener(connectionStateListener);

        try {
            cache.close();
        } catch (IOException e) {
            throw new FlinkException(
                    "Could not properly stop the HybridLeaderRetrievalService.", e);
        }

        LOG.info("Stopping HybridLeaderRetrievalService, listener={}.", leaderListener);
    }

    @Override
    public void unhandledError(String s, Throwable throwable) {
        LOG.error("Unhandled error in HybridLeaderRetrievalService: {}", s, throwable);

        useFileSystemRetrieval();
    }

    @Override
    public void nodeChanged() throws Exception {
        synchronized (lock) {
            try {
                LOG.info("Leader node has changed.");

                ChildData childData = cache.getCurrentData();

                LeaderInformation leaderInformation;

                if (childData == null) {
                    leaderInformation = LeaderInformation.empty();
                } else {
                    byte[] data = childData.getData();

                    if (data == null || data.length == 0) {
                        leaderInformation = LeaderInformation.empty();
                    } else {
                        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                                ObjectInputStream ois = new ObjectInputStream(bais)) {
                            String address = ois.readUTF();
                            UUID sessionId = (UUID) ois.readObject();

                            leaderInformation = LeaderInformation.known(sessionId, address);
                        }
                    }
                }

                // compare with the leader information retrieved from FileSystem
                if (leaderInformation.equals(getLeaderInformationFromFileSystem())) {
                    useZooKeeperRetrieval();
                    notifyIfNewLeader(leaderInformation);
                } else {
                    useFileSystemRetrieval();
                }

            } catch (Exception e) {
                LOG.error("Could not handle node changed event.", e);
                useFileSystemRetrieval();
            }
        }
    }

    private void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                LOG.info("Connected to ZooKeeper quorum. Retrieve the leader from ZooKeeper.");
                useZooKeeperRetrieval();
                break;
            case SUSPENDED:
                LOG.warn(
                        "Connection to ZooKeeper suspended. Can no longer retrieve the leader from "
                                + "ZooKeeper.");
                useFileSystemRetrieval();
                break;
            case RECONNECTED:
                LOG.info(
                        "Connection to ZooKeeper was reconnected. Leader retrieval can be restarted.");
                useZooKeeperRetrieval();
                break;
            case LOST:
                LOG.warn(
                        "Connection to ZooKeeper lost. Can no longer retrieve the leader from ZooKeeper.");
                useFileSystemRetrieval();
                break;
        }
    }

    @GuardedBy("lock")
    private void notifyIfNewLeader(LeaderInformation leaderInformation) {
        if (!leaderInformation.equals(lastLeaderInformation)) {
            if (leaderInformation.equals(LeaderInformation.empty())) {
                LOG.warn("Leader information was lost: The listener will be notified accordingly.");
            } else {
                LOG.info("New leader information: {}.", leaderInformation);
            }

            lastLeaderInformation = leaderInformation;
            leaderListener.notifyLeaderAddress(
                    leaderInformation.getLeaderAddress(), leaderInformation.getLeaderSessionID());
        }
    }

    private LeaderInformation getLeaderInformationFromFileSystem() throws Exception {
        Path leaderPath =
                new Path(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        this.pathSuffix);

        if (!fs.exists(leaderPath) || fs.getFileStatus(leaderPath).isDir()) {
            return LeaderInformation.empty();
        }

        try (FSDataInputStream inputStream = fs.open(leaderPath);
                ObjectInputStream ois = new ObjectInputStream(inputStream)) {

            String address = ois.readUTF();
            UUID sessionId = (UUID) ois.readObject();

            return LeaderInformation.known(sessionId, address);
        }
    }

    /** Retrieve the leader address as well as the current leader session ID from FileSystem. */
    private void useFileSystemRetrieval() {
        synchronized (lock) {
            if (zkAvailable) {
                zkAvailable = false;
                switchFileSystemNum++;
                executor.execute(
                        () -> {
                            while (running && !zkAvailable) {
                                try {
                                    notifyIfNewLeader(getLeaderInformationFromFileSystem());
                                } catch (Throwable e) {
                                    LOG.warn(
                                            "Could not retrieve leader information from file system",
                                            e);
                                }

                                try {
                                    Thread.sleep(pollingInterval);
                                } catch (InterruptedException e) {
                                    LOG.warn(
                                            "Retrieve leader information from file system interrupted");
                                }
                            }
                        });
            }
        }
    }

    /** Retrieve the leader address as well as the current leader session ID from ZooKeeper. */
    private void useZooKeeperRetrieval() {
        synchronized (lock) {
            if (!zkAvailable) {
                zkAvailable = true;
            }
        }
    }

    public boolean zkAvailable() {
        return zkAvailable;
    }

    public int getSwitchFileSystemNum() {
        return switchFileSystemNum;
    }
}
