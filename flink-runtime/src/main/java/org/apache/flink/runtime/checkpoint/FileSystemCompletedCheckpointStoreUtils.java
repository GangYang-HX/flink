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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.persistence.filesystem.FileSystemReaderWriterHelper;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Helper methods related to {@link FileSystemCompletedCheckpointStore}. */
public class FileSystemCompletedCheckpointStoreUtils {

    private static final Logger LOG =
            LoggerFactory.getLogger(FileSystemCompletedCheckpointStoreUtils.class);

    private FileSystemCompletedCheckpointStoreUtils() {
        // No-op.
    }

    /**
     * Extracts maximum number of retained checkpoints configuration from the passed {@link
     * Configuration}. The default value is used as a fallback if the passed value is a value larger
     * than {@code 0}.
     *
     * @param config The configuration that is accessed.
     * @param logger The {@link Logger} used for exposing the warning if the configured value is
     *     invalid.
     * @return The maximum number of retained checkpoints based on the passed {@code Configuration}.
     */
    public static int getMaximumNumberOfRetainedCheckpoints(Configuration config, Logger logger) {
        final int maxNumberOfCheckpointsToRetain =
                config.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

        if (maxNumberOfCheckpointsToRetain <= 0) {
            // warning and use 1 as the default value if the setting in
            // state.checkpoints.max-retained-checkpoints is not greater than 0.
            logger.warn(
                    "The setting for '{} : {}' is invalid. Using default value of {}",
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                    maxNumberOfCheckpointsToRetain,
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

            return CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
        }

        return maxNumberOfCheckpointsToRetain;
    }

    /** Gets the latest checkpoint from FileSystem and removes all others. */
    public static Collection<CompletedCheckpoint> retrieveCompletedCheckpoint(
            FileSystem fs,
            FileSystemReaderWriterHelper<CompletedCheckpoint> readerWriterHelper,
            Path pathPrefix)
            throws Exception {
        LOG.info("Recovering checkpoints from FileSystem.");

        if (!fs.exists(pathPrefix) || !fs.getFileStatus(pathPrefix).isDir()) {
            return Collections.emptyList();
        }

        // First get all checkpoint paths from file system
        FileStatus[] statuses = fs.listStatus(pathPrefix);
        List<Path> initialCheckpointPaths =
                Arrays.stream(statuses)
                        .map(s -> s.getPath())
                        .sorted(Comparator.comparing(Path::getPath))
                        .collect(Collectors.toList());

        int numberOfInitialCheckpoints = initialCheckpointPaths.size();

        LOG.info("Found {} checkpoints in file system.", numberOfInitialCheckpoints);

        // Try and read the state handles from storage. We try until we either successfully read
        // all of them or when we reach a stable state, i.e. when we successfully read the same set
        // of checkpoints in two tries. We do it like this to protect against transient outages
        // of the checkpoint store (for example a DFS): if the DFS comes online midway through
        // reading a set of checkpoints we would run the risk of reading only a partial set
        // of checkpoints while we could in fact read the other checkpoints as well if we retried.
        // Waiting until a stable state protects against this while also being resilient against
        // checkpoints being actually unreadable.
        //
        // These considerations are also important in the scope of incremental checkpoints, where
        // we use ref-counting for shared state handles and might accidentally delete shared state
        // of checkpoints that we don't read due to transient storage outages.
        List<CompletedCheckpoint> lastTryRetrievedCheckpoints =
                new ArrayList<>(numberOfInitialCheckpoints);
        List<CompletedCheckpoint> retrievedCheckpoints =
                new ArrayList<>(numberOfInitialCheckpoints);
        do {
            LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

            lastTryRetrievedCheckpoints.clear();
            lastTryRetrievedCheckpoints.addAll(retrievedCheckpoints);
            retrievedCheckpoints.clear();

            for (Path path : initialCheckpointPaths) {
                try {
                    CompletedCheckpoint completedCheckpoint = readerWriterHelper.read(path);
                    if (completedCheckpoint != null) {
                        retrievedCheckpoints.add(completedCheckpoint);
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Could not retrieve checkpoint, not adding to list of recovered checkpoints.",
                            e);
                }
            }

        } while (retrievedCheckpoints.size() != numberOfInitialCheckpoints
                && !CompletedCheckpoint.checkpointsMatch(
                        lastTryRetrievedCheckpoints, retrievedCheckpoints));

        if (retrievedCheckpoints.isEmpty() && numberOfInitialCheckpoints > 0) {
            throw new FlinkException(
                    "Could not read any of the "
                            + numberOfInitialCheckpoints
                            + " checkpoints from storage.");
        } else if (retrievedCheckpoints.size() != numberOfInitialCheckpoints) {
            LOG.warn(
                    "Could only fetch {} of {} checkpoints from storage.",
                    retrievedCheckpoints.size(),
                    numberOfInitialCheckpoints);
        }

        return Collections.unmodifiableList(retrievedCheckpoints);
    }
}
