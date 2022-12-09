package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.CompletableFuture;

/**
 * {@link CheckpointIDCounter} implementation using {@link FileSystem}. The current checkpoint ID
 * will be stored as a integer in the following file:
 *
 * <p>&lt;user-configured-path-prefix&gt;/&lt;job-ID&gt;/checkpoint-counter We choose to open the
 * file every time when we want to read/write the file, because the read/write operations are not
 * frequent, as checkpoint interval is long enough in common.
 */
public class FileSystemCheckpointIDCounter implements CheckpointIDCounter {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCheckpointIDCounter.class);

    private static final String CHECKPOINT_ID_COUNTER_PATH = "/checkpoint_id_counter";

    private final Path counterPath;

    private final FileSystem fs;

    private final int retries;

    public FileSystemCheckpointIDCounter(FileSystem fs, Configuration configuration, JobID jobID) {
        this.fs = fs;
        this.counterPath =
                new Path(
                        new Path(
                                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                        configuration),
                                CHECKPOINT_ID_COUNTER_PATH),
                        jobID.toString());
        this.retries =
                configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_MAX_RETRY_ATTEMPTS);
    }

    @Override
    public void start() throws Exception {
        try {
            if (!this.fs.exists(counterPath)) {
                writeToFile(1);
            }
        } catch (IOException e) {
            LOG.error("Checkpoint id counter did not exist, and we failed to create it.", e);
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
        if (jobStatus.isGloballyTerminalState()) {
            try {
                fs.delete(counterPath, false);
            } catch (IOException e) {
                LOG.error("Checkpoint id counter did not exist, and we failed to create it.", e);
                throw new FlinkRuntimeException(e);
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    @Override
    public long getAndIncrement() throws Exception {
        long currentId = readFromFile();
        long nextId = currentId + 1;
        writeToFile(nextId);
        return currentId;
    }

    @Override
    public long get() {
        return readFromFile();
    }

    @Override
    public void setCount(long newId) throws Exception {
        writeToFile(newId);
    }

    private long readFromFile() {
        int numRetries = 0;
        while (numRetries < retries) {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(fs.open(counterPath)))) {
                return Long.parseLong(reader.readLine().trim());
            } catch (IOException e) {
                LOG.warn("Failed to read checkpoint ID from {}", counterPath.getPath(), e);
                numRetries += 1;
                if (numRetries >= retries) {
                    throw new RuntimeException(e);
                }
            }
        }

        // There is no chance that this line will be reached. Just add it to make the compiler
        // happy.
        return -1;
    }

    private void writeToFile(long id) throws IOException {
        int numRetries = 0;
        while (numRetries < retries) {
            try (BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    fs.create(counterPath, FileSystem.WriteMode.OVERWRITE)))) {
                writer.append(Long.toString(id));
                return;
            } catch (IOException e) {
                LOG.warn("Failed to write checkpoint ID to {}", counterPath.getPath(), e);
                numRetries += 1;
                if (numRetries >= retries) {
                    throw e;
                }
            }
        }
    }
}
