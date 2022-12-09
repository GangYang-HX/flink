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

package org.apache.flink.runtime.checkpoint.listener;

import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * CheckpointInfoListenerUtil is mainly to use spi to discover {@link CheckpointInfoListener}.
 * This class can be called in {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator},
 * when {@link CheckpointCoordinatorConfiguration#isCheckpointInfoListenerEnabled} is true. You can use
 * "execution.checkpointing.listener.enabled=true" optional config to turn on checkpoint info listener.
 */
public class CheckpointInfoListenerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointInfoListenerUtil.class);

    private static List<CheckpointInfoListener> registeredListeners;

    public static List<CheckpointInfoListener> getRegisteredListeners(boolean enabled) {
        if (!enabled) {
            return Collections.emptyList();
        }
        if (registeredListeners == null) {
            registeredListeners = discoverListeners();
        }
        return registeredListeners;
    }

    private static List<CheckpointInfoListener> discoverListeners() {

        try {
            final List<CheckpointInfoListener> result = new ArrayList<>();
            ServiceLoader
                    .load(CheckpointInfoListener.class)
                    .iterator()
                    .forEachRemaining(result::add);
            LOG.info("There are {} listeners founded.", result.size());
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for CheckpointInfoListeners.", e);
        }
        return Collections.emptyList();
    }
}
