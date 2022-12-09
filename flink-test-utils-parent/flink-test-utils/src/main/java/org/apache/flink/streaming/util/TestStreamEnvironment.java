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

package org.apache.flink.streaming.util;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.JobExecutor;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

/**
 * A {@link StreamExecutionEnvironment} that executes its jobs on {@link MiniCluster}.
 */
public class TestStreamEnvironment extends StreamExecutionEnvironment {

	private static final boolean RANDOMIZE_CHECKPOINTING_CONFIG =
		Boolean.parseBoolean(System.getProperty("checkpointing.randomization", "false"));

	/** The job executor to use to execute environment's jobs. */
	private final JobExecutor jobExecutor;

	private final Collection<Path> jarFiles;

	private final Collection<URL> classPaths;

	private static final boolean RANDOMIZE_BUFFER_DEBLOAT_CONFIG =
			Boolean.parseBoolean(System.getProperty("buffer-debloat.randomization", "false"));

	public TestStreamEnvironment(
			JobExecutor jobExecutor,
			int parallelism,
			Collection<Path> jarFiles,
			Collection<URL> classPaths) {

		this.jobExecutor = Preconditions.checkNotNull(jobExecutor);
		this.jarFiles = Preconditions.checkNotNull(jarFiles);
		this.classPaths = Preconditions.checkNotNull(classPaths);
		getConfiguration().set(DeploymentOptions.TARGET, LocalExecutor.NAME);
		getConfiguration().set(DeploymentOptions.ATTACHED, true);

		setParallelism(parallelism);
	}

	public TestStreamEnvironment(
			JobExecutor jobExecutor,
			int parallelism) {
		this(jobExecutor, parallelism, Collections.emptyList(), Collections.emptyList());
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		final JobGraph jobGraph = streamGraph.getJobGraph();

		for (Path jarFile : jarFiles) {
			jobGraph.addJar(jarFile);
		}

		jobGraph.setClasspaths(new ArrayList<>(classPaths));

		return jobExecutor.executeJobBlocking(jobGraph);
	}

	// ------------------------------------------------------------------------

	/**
	 * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
	 * the given cluster with the given default parallelism and the specified jar files and class
	 * paths.
	 *
	 * @param jobExecutor The executor to execute the jobs on
	 * @param parallelism The default parallelism for the test programs.
	 * @param jarFiles Additional jar files to execute the job with
	 * @param classpaths Additional class paths to execute the job with
	 */
	public static void setAsContext(
			final JobExecutor jobExecutor,
			final int parallelism,
			final Collection<Path> jarFiles,
			final Collection<URL> classpaths) {

		StreamExecutionEnvironmentFactory factory = () -> {
			TestStreamEnvironment environment = new TestStreamEnvironment(
					jobExecutor,
					parallelism,
					jarFiles,
					classpaths);

			if (RANDOMIZE_BUFFER_DEBLOAT_CONFIG) {
				PseudoRandomValueSelector.randomize(
						"StreamEnvTest",
						environment.getConfiguration(),
						TaskManagerOptions.BUFFER_DEBLOAT_ENABLED,
						true,
						false);
			}
			randomize(environment.getConfiguration());

			return environment;
		};

		initializeContextEnvironment(factory);
	}

    /**
     * Randomizes configuration on test case level even if mini cluster is used in a class rule.
     *
     * <p>Note that only unset properties are randomized.
     *
     * @param conf the configuration to randomize
     */
    private static void randomize(Configuration conf) {
        if (RANDOMIZE_CHECKPOINTING_CONFIG) {
            final String testName = "TestNameProvider.getCurrentTestName()";
            final PseudoRandomValueSelector valueSelector =
                    PseudoRandomValueSelector.create(testName != null ? testName : "unknown");
            valueSelector.select(conf, ExecutionCheckpointingOptions.ENABLE_UNALIGNED, true, false);
            valueSelector.select(
                    conf,
                    ExecutionCheckpointingOptions.ALIGNMENT_TIMEOUT,
                    Duration.ofSeconds(0),
                    Duration.ofMillis(100),
                    Duration.ofSeconds(2));
        }
    }

	/**
	 * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
	 * the given cluster with the given default parallelism.
	 *
	 * @param jobExecutor The executor to execute the jobs on
	 * @param parallelism The default parallelism for the test programs.
	 */
	public static void setAsContext(final JobExecutor jobExecutor, final int parallelism) {
		setAsContext(
			jobExecutor,
			parallelism,
			Collections.emptyList(),
			Collections.emptyList());
	}

	/**
	 * Resets the streaming context environment to null.
	 */
	public static void unsetAsContext() {
		resetContextEnvironment();
	}

	public static class PseudoRandomValueSelector {

		private final Function<Integer, Integer> randomValueSupplier;

		private static final long GLOBAL_SEED = (long) getGlobalSeed().hashCode() << 32;

		private PseudoRandomValueSelector(Function<Integer, Integer> randomValueSupplier) {
			this.randomValueSupplier = randomValueSupplier;
		}

		public <T> void select(Configuration configuration, ConfigOption<T> option, T... alternatives) {
			if (configuration.contains(option)) {
				return;
			}
			final int choice = randomValueSupplier.apply(alternatives.length);
			T value = alternatives[choice];
			configuration.set(option, value);
		}

		public static PseudoRandomValueSelector create(Object entryPointSeed) {
			final long combinedSeed = GLOBAL_SEED | entryPointSeed.hashCode();
			final Random random = new Random(combinedSeed);
			return new PseudoRandomValueSelector(random::nextInt);
		}

		private static String getGlobalSeed() {
			// manual seed or set by maven
			final String seed = System.getProperty("test.randomization.seed");
			if (seed != null && !seed.isEmpty()) {
				return seed;
			}

			// Read with git command (if installed)
			final Optional<String> gitCommitId = getGitCommitId();
			return gitCommitId.orElseGet(EnvironmentInformation::getGitCommitId);
		}

		public static Optional<String> getGitCommitId() {
			try {
				Process process = new ProcessBuilder("git", "rev-parse", "HEAD").start();
				InputStream input = process.getInputStream();
				final String commit = IOUtils.toString(input, Charset.defaultCharset()).trim();
				if (commit.matches("[a-f0-9]{40}")) {
					return Optional.of(commit);
				}
			} catch (IOException e) {
				//
			}
			return Optional.empty();
		}

		public static <T> void randomize(String testName, Configuration conf, ConfigOption<T> option, T... t1) {
			final PseudoRandomValueSelector valueSelector = PseudoRandomValueSelector.create(testName);
			valueSelector.select(conf, option, t1);
		}
	}
}
