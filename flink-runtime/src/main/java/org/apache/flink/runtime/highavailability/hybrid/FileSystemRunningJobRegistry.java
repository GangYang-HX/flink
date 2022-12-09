package org.apache.flink.runtime.highavailability.hybrid;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RunningJobsRegistry} implementation using {@link FileSystem}. The current status of the
 * job will be stored as a string in the following file:
 *
 * <pre>
 *     &lt;ha-storage-path&gt;/&lt;cluster-ID&gt;/running_job_registry/&lt;job-ID&gt;
 * </pre>
 *
 * We choose to open the file every time when we want to read/write the file, because the read/write
 * operations are not frequent.
 */
public class FileSystemRunningJobRegistry implements RunningJobsRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemRunningJobRegistry.class);

	private static final String RUNNING_JOB_REGISTRY_PATH = "/running_job_registry";

	private final FileSystem fs;

	private final Path prefixPath;

	private final int maxRetries;

	public FileSystemRunningJobRegistry(FileSystem fs, Configuration configuration) {
		this.fs = fs;
		this.prefixPath = new Path(
			HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration),
			RUNNING_JOB_REGISTRY_PATH);
		this.maxRetries = configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_MAX_RETRY_ATTEMPTS);

		try {
			if (!this.fs.exists(prefixPath)) {
				fs.mkdirs(prefixPath);
			}
		} catch (IOException e) {
			LOG.error("Job running registry directory did not exist, and we failed to create it.", e);
			throw new FlinkRuntimeException(e);
		}
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);
		writeEnumToFile(jobID, JobSchedulingStatus.RUNNING);
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);
		writeEnumToFile(jobID, JobSchedulingStatus.DONE);
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		checkNotNull(jobID);
		return readFromFile(jobID);
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		Path jobRunningStatusPath = getPath(jobID);

		if (!fs.exists(jobRunningStatusPath)) {
			LOG.warn("{} did not exist when trying to clear job running status.", jobRunningStatusPath.getPath());
			return;
		}

		if (fs.getFileStatus(jobRunningStatusPath).isDir()) {
			LOG.warn("{} is expected to be a file, but is a directory.", jobRunningStatusPath.getPath());
			return;
		}

		fs.delete(jobRunningStatusPath, false);
	}

	private Path getPath(JobID jobId) {
		return new Path(this.prefixPath, jobId.toString());
	}

	private void writeEnumToFile(JobID jobId, JobSchedulingStatus status) throws IOException {
		Path jobRunningStatusPath = getPath(jobId);

		int retries = 0;
		while (retries < maxRetries) {
			try (BufferedWriter writer =
					 new BufferedWriter(
						 new OutputStreamWriter(
							 fs.create(jobRunningStatusPath, FileSystem.WriteMode.OVERWRITE)))) {
				writer.write(status.name());
				return;
			} catch (IOException ioe) {
				retries += 1;
				LOG.warn("Failed to write job running status to {}", jobRunningStatusPath.getPath(), ioe);
				if (retries >= maxRetries) {
					throw ioe;
				}
			}
		}
	}

	private JobSchedulingStatus readFromFile(JobID jobId) throws IOException {
		Path jobRunningStatusPath = getPath(jobId);

		int retries = 0;
		while (retries < maxRetries) {
			try {
				if (!fs.exists(jobRunningStatusPath)) {
					return JobSchedulingStatus.PENDING;
				}

				if (fs.getFileStatus(jobRunningStatusPath).isDir()) {
					throw new IllegalStateException(
						"Job running registry path ("
							+ jobRunningStatusPath.getPath()
							+ ") is expected to be a file, but is a directory.");
				}

				try(BufferedReader reader =
					new BufferedReader(new InputStreamReader(fs.open(jobRunningStatusPath)))) {
					return JobSchedulingStatus.valueOf(reader.readLine().trim());
				}
			} catch (IOException ioe) {
				retries += 1;
				LOG.warn("Failed to write job running status to {}", jobRunningStatusPath.getPath(), ioe);
				if (retries >= maxRetries) {
					throw ioe;
				}
			}
		}

		return JobSchedulingStatus.PENDING;
	}
}
