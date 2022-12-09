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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class with method related to job execution.
 */
public class PipelineExecutorUtils {

	private static final Logger LOG = LoggerFactory.getLogger(PipelineExecutorUtils.class);

	/**
	 * Creates the {@link JobGraph} corresponding to the provided {@link Pipeline}.
	 *
	 * @param pipeline the pipeline whose job graph we are computing
	 * @param configuration the configuration with the necessary information such as jars and
	 *                         classpaths to be included, the parallelism of the job and potential
	 *                         savepoint settings used to bootstrap its state.
	 * @return the corresponding {@link JobGraph}.
	 */
	public static JobGraph getJobGraph(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws MalformedURLException {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil
				.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());

		configuration
				.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
				.ifPresent(strJobID -> jobGraph.setJobID(JobID.fromHexString(strJobID)));

		jobGraph.addJars(filterJarsIfNecessary(executionConfigAccessor.getJars(), configuration));
		jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

		return jobGraph;
	}

	private static List<URL> filterJarsIfNecessary(List<URL> jars, Configuration config) {
		if (config.get(PipelineOptionsInternal.REMOTE_JARS) == null) {
			return jars;
		}

		// Compare client jars from `PipelineOptions.JARS` and remote jars from `PipelineOptionsInternal.REMOTE_JARS`,
		// exclude jar from client jars which is also included in remote jars
		// thus, we can decrease two steps for a remote existed user jar:
		// 1. upload it to BlobServer in client
		// 2. download it from BlobServer when starting a Task
		List<String> remoteJars = ConfigUtils.decodeListFromConfig(
				config, PipelineOptionsInternal.REMOTE_JARS, Path::new)
			.stream()
			.map(Path::getName)
			.collect(Collectors.toList());

		try {
			List<URL> localJars = ConfigUtils.decodeListFromConfig(config, PipelineOptions.JARS, URL::new);

			List<URL> filteredJars = localJars.stream()
				.filter(jar -> {
					try {
						org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(jar.toURI());
						return !remoteJars.contains(path.getName());
					} catch (URISyntaxException e) {
						LOG.warn("URL is invalid. This should not happen.", e);
						return true;
					}
				})
				.collect(Collectors.toList());

			return filteredJars;
		} catch (MalformedURLException e) {
			LOG.warn("URL is malformed. This should not happen.", e);
		}

		return jars;
	}

}
