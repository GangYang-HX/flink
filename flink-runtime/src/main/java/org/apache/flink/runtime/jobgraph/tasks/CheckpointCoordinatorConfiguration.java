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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration settings for the {@link CheckpointCoordinator}. This includes the checkpoint
 * interval, the checkpoint timeout, the pause between checkpoints, the maximum number of
 * concurrent checkpoints and settings for externalized checkpoints.
 */
public class CheckpointCoordinatorConfiguration implements Serializable {

	private static final long serialVersionUID = 2L;

	public static final long MINIMAL_CHECKPOINT_TIME = 10;

	private static final boolean SHOULD_SKIP_MAX_PARALLELISM_CHECK_DEFAULT = false;

	private static final boolean ENABLE_AUTO_ADJUST_MAX_PARALLELISM_CHECK_DEFAULT = false;

	private static final boolean IS_REGIONAL_CHECKPOINT_ENABLED_DEFAULT = false;

	private static final int MAX_TOLERABLE_CONSECUTIVE_FAILURES_OR_EXPIRATIONS_DEFAULT = 3;

	private static final double MAX_TOLERABLE_CONSECUTIVE_FAILURE_OR_EXPIRATION_RATIO_DEFAULT = 0.5;

	private static final boolean EXCLUDE_BROADCASTED_VERTEX_DEFAULT = false;


	private final long checkpointInterval;

	private final long checkpointTimeout;

	private final long minPauseBetweenCheckpoints;

	private final int maxConcurrentCheckpoints;

	private final int tolerableCheckpointFailureNumber;

	/** Settings for what to do with checkpoints when a job finishes. */
	private final CheckpointRetentionPolicy checkpointRetentionPolicy;

	/**
	 * Flag indicating whether exactly once checkpoint mode has been configured.
	 * If <code>false</code>, at least once mode has been configured. This is
	 * not a necessary attribute, because the checkpointing mode is only relevant
	 * for the stream tasks, but we expose it here to forward it to the web runtime
	 * UI.
	 */
	private final boolean isExactlyOnce;

	private final boolean isPreferCheckpointForRecovery;

	private final boolean isUnalignedCheckpointsEnabled;

	private final boolean skipMaxParallelismCheck;

	private final boolean isCheckpointInfoListenerEnabled;

	private final boolean autoAdjustMaxParallelism;

	private final boolean isRegionalCheckpointEnabled;

	private int maxTolerableConsecutiveFailuresOrExpirations;

	private double maxTolerableFailureOrExpirationRatio;

	private boolean excludeBroadcastedVertex;

	private final long alignmentTimeout;

	/**
	 * @deprecated use {@link #builder()}.
	 */
	@Deprecated
	@VisibleForTesting
	public CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isUnalignedCheckpoint,
			boolean isPreferCheckpointForRecovery,
			int tolerableCpFailureNumber) {
		this(
				checkpointInterval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				checkpointRetentionPolicy,
				isExactlyOnce,
				isPreferCheckpointForRecovery,
				tolerableCpFailureNumber,
				SHOULD_SKIP_MAX_PARALLELISM_CHECK_DEFAULT,
				ENABLE_AUTO_ADJUST_MAX_PARALLELISM_CHECK_DEFAULT,
				IS_REGIONAL_CHECKPOINT_ENABLED_DEFAULT,
				MAX_TOLERABLE_CONSECUTIVE_FAILURES_OR_EXPIRATIONS_DEFAULT,
				MAX_TOLERABLE_CONSECUTIVE_FAILURE_OR_EXPIRATION_RATIO_DEFAULT,
				EXCLUDE_BROADCASTED_VERTEX_DEFAULT,
				isUnalignedCheckpoint,
				0,
				false);
	}

	private CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isPreferCheckpointForRecovery,
			int tolerableCpFailureNumber,
			boolean skipMaxParallelismCheck,
			boolean autoAdjustMaxParallelism,
			boolean isRegionalCheckpointEnabled,
			int maxTolerableConsecutiveFailuresOrExpirations,
			double maxTolerableFailureOrExpirationRatio,
			boolean excludeBroadcastedVertex,
			boolean isUnalignedCheckpointsEnabled,
			long alignmentTimeout,
			boolean isCheckpointInfoListenerEnabled) {

		// sanity checks
		if (checkpointInterval < MINIMAL_CHECKPOINT_TIME || checkpointTimeout < MINIMAL_CHECKPOINT_TIME ||
				minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1 ||
				tolerableCpFailureNumber < 0) {
			throw new IllegalArgumentException();
		}
		Preconditions.checkArgument(!isUnalignedCheckpointsEnabled || maxConcurrentCheckpoints <= 1,
				"maxConcurrentCheckpoints can't be > 1 if UnalignedCheckpoints enabled");

		this.checkpointInterval = checkpointInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.checkpointRetentionPolicy = Preconditions.checkNotNull(checkpointRetentionPolicy);
		this.isExactlyOnce = isExactlyOnce;
		this.isPreferCheckpointForRecovery = isPreferCheckpointForRecovery;
		this.tolerableCheckpointFailureNumber = tolerableCpFailureNumber;
		this.isUnalignedCheckpointsEnabled = isUnalignedCheckpointsEnabled;
		this.skipMaxParallelismCheck = skipMaxParallelismCheck;
		this.autoAdjustMaxParallelism = autoAdjustMaxParallelism;
		this.isRegionalCheckpointEnabled = isRegionalCheckpointEnabled;
		this.maxTolerableConsecutiveFailuresOrExpirations = maxTolerableConsecutiveFailuresOrExpirations;
		this.maxTolerableFailureOrExpirationRatio = maxTolerableFailureOrExpirationRatio;
		this.excludeBroadcastedVertex = excludeBroadcastedVertex;
		this.alignmentTimeout = alignmentTimeout;
		this.isCheckpointInfoListenerEnabled = isCheckpointInfoListenerEnabled;
	}

	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	public long getMinPauseBetweenCheckpoints() {
		return minPauseBetweenCheckpoints;
	}

	public int getMaxConcurrentCheckpoints() {
		return maxConcurrentCheckpoints;
	}

	public CheckpointRetentionPolicy getCheckpointRetentionPolicy() {
		return checkpointRetentionPolicy;
	}

	public boolean isExactlyOnce() {
		return isExactlyOnce;
	}

	public boolean isPreferCheckpointForRecovery() {
		return isPreferCheckpointForRecovery;
	}

	public int getTolerableCheckpointFailureNumber() {
		return tolerableCheckpointFailureNumber;
	}

	public boolean isUnalignedCheckpointsEnabled() {
		return isUnalignedCheckpointsEnabled;
	}

	public boolean skipMaxParallelismCheck() {
		return skipMaxParallelismCheck;
	}

	public boolean isCheckpointInfoListenerEnabled() {
		return isCheckpointInfoListenerEnabled;
	}

	public boolean autoAdjustMaxParallelism() {
		return autoAdjustMaxParallelism;
	}

	public boolean isRegionalCheckpointEnabled() {
		return isRegionalCheckpointEnabled;
	}

	public int getMaxTolerableConsecutiveFailuresOrExpirations() {
		return maxTolerableConsecutiveFailuresOrExpirations;
	}

	public double getMaxTolerableFailureOrExpirationRatio() {
		return maxTolerableFailureOrExpirationRatio;
	}

	public boolean getExcludeBroadcastedVertex() {
		return excludeBroadcastedVertex;
	}

	public long getAlignmentTimeout() {
		return alignmentTimeout;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointCoordinatorConfiguration that = (CheckpointCoordinatorConfiguration) o;
		return checkpointInterval == that.checkpointInterval &&
				checkpointTimeout == that.checkpointTimeout &&
				minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints &&
				maxConcurrentCheckpoints == that.maxConcurrentCheckpoints &&
				isExactlyOnce == that.isExactlyOnce &&
				isUnalignedCheckpointsEnabled == that.isUnalignedCheckpointsEnabled &&
				checkpointRetentionPolicy == that.checkpointRetentionPolicy &&
				isPreferCheckpointForRecovery == that.isPreferCheckpointForRecovery &&
				tolerableCheckpointFailureNumber == that.tolerableCheckpointFailureNumber &&
				this.skipMaxParallelismCheck == that.skipMaxParallelismCheck &&
				this.autoAdjustMaxParallelism == that.autoAdjustMaxParallelism &&
				this.isRegionalCheckpointEnabled == that.isRegionalCheckpointEnabled &&
				this.maxTolerableConsecutiveFailuresOrExpirations == that.maxTolerableConsecutiveFailuresOrExpirations &&
				this.maxTolerableFailureOrExpirationRatio == that.maxTolerableFailureOrExpirationRatio&&
				isCheckpointInfoListenerEnabled == that.isCheckpointInfoListenerEnabled;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				checkpointInterval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				checkpointRetentionPolicy,
				isExactlyOnce,
				isUnalignedCheckpointsEnabled,
				isPreferCheckpointForRecovery,
				tolerableCheckpointFailureNumber,
				skipMaxParallelismCheck,
				autoAdjustMaxParallelism,
				isRegionalCheckpointEnabled,
				maxTolerableConsecutiveFailuresOrExpirations,
				maxTolerableFailureOrExpirationRatio);
	}

	@Override
	public String toString() {
		return "JobCheckpointingConfiguration{" +
				"checkpointInterval=" + checkpointInterval +
				", checkpointTimeout=" + checkpointTimeout +
				", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints +
				", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints +
				", checkpointRetentionPolicy=" + checkpointRetentionPolicy +
				", isExactlyOnce=" + isExactlyOnce +
				", isUnalignedCheckpoint=" + isUnalignedCheckpointsEnabled +
				", isPreferCheckpointForRecovery=" + isPreferCheckpointForRecovery +
				", tolerableCheckpointFailureNumber=" + tolerableCheckpointFailureNumber +
				", skipMaxParallelismCheck=" + skipMaxParallelismCheck +
				", skipMaxParallelismCheck=" + autoAdjustMaxParallelism +
				", isRegionalCheckpointEnabled=" + isRegionalCheckpointEnabled +
				", maxTolerableConsecutiveFailuresOrExpirations=" + maxTolerableConsecutiveFailuresOrExpirations +
				", maxTolerableFailureOrExpirationRatio=" + maxTolerableFailureOrExpirationRatio +
				", isCheckpointInfoListenerEnabled=" + isCheckpointInfoListenerEnabled +
				'}';
	}

	public static CheckpointCoordinatorConfigurationBuilder builder() {
		return new CheckpointCoordinatorConfigurationBuilder();
	}

	/**
	 * {@link CheckpointCoordinatorConfiguration} builder.
	 */
	public static class CheckpointCoordinatorConfigurationBuilder {
		private long checkpointInterval = MINIMAL_CHECKPOINT_TIME;
		private long checkpointTimeout = MINIMAL_CHECKPOINT_TIME;
		private long minPauseBetweenCheckpoints;
		private int maxConcurrentCheckpoints = 1;
		private CheckpointRetentionPolicy checkpointRetentionPolicy = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		private boolean isExactlyOnce = true;
		private boolean isPreferCheckpointForRecovery = true;
		private int tolerableCheckpointFailureNumber;
		private boolean isUnalignedCheckpointsEnabled;
		private boolean skipMaxParallelismCheck;
		private boolean autoAdjustMaxParallelism;
		private boolean isRegionalCheckpointEnabled;
		private int maxTolerableConsecutiveFailuresOrExpirations;
		private double maxTolerableFailureOrExpirationRatio;
		private boolean excludeBroadcastedVertex;
		private boolean isCheckpointInfoListenerEnabled;
		private long alignmentTimeout = 0;

		public CheckpointCoordinatorConfiguration build() {
			return new CheckpointCoordinatorConfiguration(
					checkpointInterval,
					checkpointTimeout,
					minPauseBetweenCheckpoints,
					maxConcurrentCheckpoints,
					checkpointRetentionPolicy,
					isExactlyOnce,
					isPreferCheckpointForRecovery,
					tolerableCheckpointFailureNumber,
					skipMaxParallelismCheck,
					autoAdjustMaxParallelism,
					isRegionalCheckpointEnabled,
					maxTolerableConsecutiveFailuresOrExpirations,
					maxTolerableFailureOrExpirationRatio,
					excludeBroadcastedVertex,
					isUnalignedCheckpointsEnabled,
					alignmentTimeout,
					isCheckpointInfoListenerEnabled);
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointInterval(long checkpointInterval) {
			this.checkpointInterval = checkpointInterval;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointTimeout(long checkpointTimeout) {
			this.checkpointTimeout = checkpointTimeout;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
			this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
			this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointRetentionPolicy(CheckpointRetentionPolicy checkpointRetentionPolicy) {
			this.checkpointRetentionPolicy = checkpointRetentionPolicy;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setExactlyOnce(boolean exactlyOnce) {
			isExactlyOnce = exactlyOnce;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setPreferCheckpointForRecovery(boolean preferCheckpointForRecovery) {
			isPreferCheckpointForRecovery = preferCheckpointForRecovery;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setTolerableCheckpointFailureNumber(int tolerableCheckpointFailureNumber) {
			this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setUnalignedCheckpointsEnabled(boolean unalignedCheckpointsEnabled) {
			isUnalignedCheckpointsEnabled = unalignedCheckpointsEnabled;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder shouldSkipMaxParallelismCheck(boolean skipMaxParallelismCheck) {
			this.skipMaxParallelismCheck = skipMaxParallelismCheck;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder enableAutoAdjustMaxParallelism(boolean autoAdjustMaxParallelism) {
			this.autoAdjustMaxParallelism = autoAdjustMaxParallelism;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setRegionalCheckpointEnabled(boolean regionalCheckpointEnabled) {
			this.isRegionalCheckpointEnabled = regionalCheckpointEnabled;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setMaxTolerableConsecutiveFailuresOrExpirations(
				int maxTolerableConsecutiveFailuresOrExpirations) {
			this.maxTolerableConsecutiveFailuresOrExpirations = maxTolerableConsecutiveFailuresOrExpirations;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setMaxTolerableFailureOrExpirationRatio(
				double maxTolerableFailureOrExpirationRatio) {
			this.maxTolerableFailureOrExpirationRatio = maxTolerableFailureOrExpirationRatio;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setExcludeBroadcastedVertex(boolean excludeBroadcastedVertex) {
			this.excludeBroadcastedVertex = excludeBroadcastedVertex;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setAlignmentTimeout(long alignmentTimeout) {
			this.alignmentTimeout = alignmentTimeout;
			return this;
		}

		public CheckpointCoordinatorConfigurationBuilder setCheckpointInfoListenerEnabled(boolean isCheckpointInfoListenerEnabled) {
			this.isCheckpointInfoListenerEnabled = isCheckpointInfoListenerEnabled;
			return this;
		}
	}
}
