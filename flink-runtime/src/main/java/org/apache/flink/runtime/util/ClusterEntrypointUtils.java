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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility class for {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}.
 */
public final class ClusterEntrypointUtils {

	private ClusterEntrypointUtils() {
		throw new UnsupportedOperationException("This class should not be instantiated.");
	}

	/**
	 * Tries to find the user library directory.
	 *
	 * @return the user library directory if it exits, returns {@link Optional#empty()} if there is none
	 */
	public static Optional<File> tryFindUserLibDirectory() {
		final File flinkHomeDirectory = deriveFlinkHomeDirectoryFromLibDirectory();
		final File usrLibDirectory = new File(flinkHomeDirectory, ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);

		if (!usrLibDirectory.isDirectory()) {
			return Optional.empty();
		}
		return Optional.of(usrLibDirectory);
	}

	/**
	 * Get the relative user lib directory. Not check whether the dir exists here.
	 *
	 * @param configuration The configuration to use.
	 * @return The String format of relative user lib directory.
	 */
	public static String getRelativeUserLibDirectory(Configuration configuration) {
		return configuration.get(DeploymentOptionsInternal.SYSTEM_CLASSPATH_INCLUDES_USER_JAR) ?
			Path.CUR_DIR :
			ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR;
	}

    /**
     * Tries to find the user library path.
     *
     * @param configuration The configuration to use.
     * @return The {@link java.nio.file.Path} Optional. If the use lib dir exists, return a real
     *     path, otherwise return empty optional.
     */
    public static Optional<java.nio.file.Path> tryFindUserLibPath(Configuration configuration) {
		final java.nio.file.Path userLibPath = Paths.get(
			FileUtils.getCurrentWorkingDirectory().toString(),
			getRelativeUserLibDirectory(configuration));

		if (Files.isDirectory(userLibPath)) {
			return Optional.of(userLibPath);
		}

		return Optional.empty();
	}

	@Nullable
	private static File deriveFlinkHomeDirectoryFromLibDirectory() {
		final String libDirectory = System.getenv().get(ConfigConstants.ENV_FLINK_LIB_DIR);

		if (libDirectory == null) {
			return null;
		} else {
			return new File(libDirectory).getParentFile();
		}
	}

	/**
	 * Gets and verify the io-executor pool size based on configuration.
	 *
	 * @param config The configuration to read.
	 * @return The legal io-executor pool size.
	 */
	public static int getPoolSize(Configuration config) {
		final int poolSize = config.getInteger(ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE, 4 * Hardware.getNumberCPUCores());
		Preconditions.checkArgument(poolSize > 0,
			String.format("Illegal pool size (%s) of io-executor, please re-configure '%s'.",
				poolSize, ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key()));
		return poolSize;
	}

    /**
     * If pipeline jars are set and are also existed in HDFS, they have already been downloaded to
     * local when starting JobManager or {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}.
     * So we don't need to download these jars again when starting JobMaster and Task but just set
     * their paths to user classpath instead.
     */
    public static Collection<URL> addUserJarsToClasspath(
            Configuration configuration, Collection<URL> classpaths) throws IOException {
		Optional<java.nio.file.Path> userLibPath = tryFindUserLibPath(configuration);

		if (userLibPath.isPresent()) {
			final List<String> jarUrls = ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, Path::new)
				.stream()
				.map(Path::getName)
				.collect(Collectors.toList());

			final Collection<URL> relativeJarURLs =
				FileUtils.listFilesInDirectory(
						userLibPath.get(), path -> jarUrls.contains(path.getFileName().toString()), false)
					.stream()
					.map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
					.collect(Collectors.toList());

			relativeJarURLs.addAll(classpaths);

			return Collections.unmodifiableCollection(relativeJarURLs);
		} else {
			return classpaths;
		}
	}

}
