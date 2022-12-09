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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.NodeCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Leader election service for Yarn. The election is more or less useless, because there always
 * exists at most one running JobManager on a Yarn cluster which will always be elected.
 *
 * <p>The real usage for this election service is both writing data into Zookeeper and the external
 * FileSystem, with the fact that writing into the FileSystem is a must while writing into Zookeeper
 * is a try-best.
 *
 * <p>We don't need to set lock in this class, because only one instance will be existed, and if
 * existing more than one instance, it means wrong HA config and need to be rightly configured.
 */
public class HybridLeaderElectionService implements LeaderElectionService, NodeCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(HybridLeaderElectionService.class);

    /** Client to the ZooKeeper quorum. */
    private final CuratorFramework client;

    private final FileSystem fs;

    private final Configuration configuration;

    /** leader path suffix of the current leader information. */
    private final String pathSuffix;

    /** The leader contender which applies for leadership. */
    private volatile LeaderContender leaderContender;

    /** Curator recipe to watch changes of a specific ZooKeeper node. */
    private final NodeCache cache;

    private final Executor executor;

    private volatile boolean running;

    /** If ZooKeeper's leader node has been written leader information successfully. */
    private volatile boolean dataWrittenToZK;

    private LeaderInformation leaderInformation;

    public HybridLeaderElectionService(
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
    }

    @Override
    public void start(LeaderContender contender) throws Exception {
        Preconditions.checkNotNull(contender, "Contender must not be null.");
        Preconditions.checkState(leaderContender == null, "Contender was already set.");

        leaderContender = contender;
        running = true;

        cache.getListenable().addListener(this);
        cache.start();

        // only exists one contender, so grand leadership at start
        UUID leaderSessionID = UUID.randomUUID();
        LOG.info(
                "Grant leadership to contender {} with session ID {}.",
                leaderContender.getDescription(),
                leaderSessionID);

        leaderContender.grantLeadership(leaderSessionID);

        LOG.info(
                "Starting HybridLeaderElectionService, contender={}, session ID={}.",
                contender,
                leaderSessionID);
    }

    @Override
    public void stop() throws Exception {
        if (!running) {
            return;
        }

        running = false;

        try {
            // delete file system path
            Path leaderPath =
                    new Path(
                            HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                    configuration),
                            this.pathSuffix);
            fs.delete(leaderPath, false);

            // delete ZooKeeper path
            client.delete().forPath(pathSuffix);

            LOG.info(
                    "Stopping HybridLeaderElectionService success, contender={}.", leaderContender);
        } catch (Exception e) {
            LOG.error(
                    "Stopping HybridLeaderElectionService error, contender={}.",
                    leaderContender,
                    e);
        }
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        LOG.info("Confirm leader session ID {} for leader {}.", leaderSessionID, leaderAddress);
        Preconditions.checkNotNull(leaderSessionID);

        leaderInformation = LeaderInformation.known(leaderSessionID, leaderAddress);
        writeLeaderInformation(leaderInformation);
    }

    /**
     * Writes the current leader's address as well the given leader session ID to FileSystem and
     * ZooKeeper. Writing to the FileSystem will block while writing to ZooKeeper will run
     * asynchronously and if ZooKeeper catch error, it will retry until writing succeed.
     */
    protected void writeLeaderInformation(LeaderInformation leaderInformation) {
        LOG.info("Write leader information: {}", leaderInformation);

        writeLeaderInformationToFileSystem(leaderInformation);
        executor.execute(() -> writeLeaderInformationToZooKeeper(leaderInformation));
    }

    private void writeLeaderInformationToFileSystem(LeaderInformation leaderInformation) {
        assert (running);

        if (leaderInformation.isEmpty()) {
            return;
        }

        Preconditions.checkNotNull(leaderInformation.getLeaderSessionID());
        Path leaderPath =
                new Path(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        this.pathSuffix);

        int maxRetry =
                configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_MAX_RETRY_ATTEMPTS);
        int retryInterval =
                configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_RETRY_WAIT);

        int numRetries = 0;
        while (numRetries < maxRetry) {
            try (FSDataOutputStream outputStream =
                            fs.create(leaderPath, FileSystem.WriteMode.OVERWRITE);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
                objectOutputStream.writeUTF(leaderInformation.getLeaderAddress());
                objectOutputStream.writeObject(leaderInformation.getLeaderSessionID());
                return;
            } catch (IOException e) {
                numRetries += 1;
                LOG.error(
                        "write leader information to file system error, retry={}, maxRetry={}",
                        numRetries,
                        maxRetry,
                        e);

                if (numRetries < maxRetry) {
                    try {
                        Thread.sleep(retryInterval);
                    } catch (InterruptedException ie) {
                        LOG.warn("Write leader information interrupted.", ie);
                    }
                } else {
                    leaderContender.handleError(
                            new FlinkException(
                                    "Could not write leader address and leader session ID to FileSystem.",
                                    e));
                }
            }
        }
    }

    private void writeLeaderInformationToZooKeeper(LeaderInformation leaderInformation) {

        // retry until write success
        while (running && !dataWrittenToZK) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos)) {

                oos.writeUTF(leaderInformation.getLeaderAddress());
                oos.writeObject(leaderInformation.getLeaderSessionID());

                dataWrittenToZK =
                        ZooKeeperUtils.writeEphemeralNode(client, pathSuffix, baos.toByteArray());
            } catch (Throwable e) {
                LOG.warn("Could not write leaderInformation to ZooKeeper.", e);
            }
        }
    }

    /**
     * Normally, node only changed once when the first time writing data to the ephemeral leader
     * node, and this method do nothing for this case.
     *
     * <p>But there exists little chance that this session with ZooKeeper might be lost because of
     * accidental network instability, thus the ephemeral leader node would be deleted. For this
     * case, we must ensure rewrite the leader information to the leader node.
     */
    @Override
    public void nodeChanged() throws Exception {
        LOG.info("Leader node has changed.");

        ChildData childData = cache.getCurrentData();
        if (childData == null && dataWrittenToZK) {
            dataWrittenToZK = false;
            executor.execute(() -> writeLeaderInformationToZooKeeper(leaderInformation));
        }
    }

    @Override
    public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
        return true;
    }

    @VisibleForTesting
    UUID getLeaderSessionID() {
        return leaderInformation.getLeaderSessionID();
    }
}
