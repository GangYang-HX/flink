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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.HybridLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nonnull;

import java.util.concurrent.Executors;

public class HybridClientHAServices implements ClientHighAvailabilityServices {

    private static final String REST_SERVER_LEADER_PATH = "/leader/rest_server";

    private final CuratorFramework client;
    private final FileSystem fs;
    private final Configuration configuration;

    public HybridClientHAServices(
            @Nonnull CuratorFramework client,
            @Nonnull FileSystem fs,
            @Nonnull Configuration configuration) {
        this.client = client;
        this.fs = fs;
        this.configuration = configuration;
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        return new HybridLeaderRetrievalService(
                client,
                fs,
                Executors.newCachedThreadPool(new ExecutorThreadFactory("hybrid-ha-io")),
                configuration,
                REST_SERVER_LEADER_PATH);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
