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
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TolerableExceptionHandler;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class HybridHaServicesFactory implements HighAvailabilityServicesFactory {

    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor)
            throws Exception {
        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration,
                        error ->
                                TolerableExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), error));
        FileSystem fs = HighAvailabilityServicesUtils.getFileSystem(configuration);
        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);

        // there perhaps exists long-term retry io operation, for example, when ZooKeeper crashed,
        // so we create a new executor instead of using the old one.
        Executor haExecutor =
                Executors.newCachedThreadPool(new ExecutorThreadFactory("hybrid-ha-io"));

        return new HybridHaServices(
                curatorFrameworkWrapper.asCuratorFramework(),
                fs,
                haExecutor,
                configuration,
                blobStoreService);
    }

    @Override
    public ClientHighAvailabilityServices createClientHAServices(Configuration configuration)
            throws Exception {
        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration,
                        error ->
                                TolerableExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), error));
        FileSystem fs = HighAvailabilityServicesUtils.getFileSystem(configuration);

        return new HybridClientHAServices(
                curatorFrameworkWrapper.asCuratorFramework(), fs, configuration);
    }
}
