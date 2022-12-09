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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Restart strategy which tries to restart a fixed number of times with a fixed backoff time in between.
 */
public class FixedDelayRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(FixedDelayRestartBackoffTimeStrategy.class);

	public static final long DISABLE_RESETTING_PERIOD = 0;

	private final long resetRestartAttemptPeriodMs;

	private static final ScheduledExecutorService scheduleExecutor = Executors.newSingleThreadScheduledExecutor();

	private final int maxNumberRestartAttempts;

	private final long backoffTimeMS;

	private final String strategyString;

	private AtomicInteger currentRestartAttempt;

	FixedDelayRestartBackoffTimeStrategy(
			int maxNumberRestartAttempts, long backoffTimeMS, long resetRestartAttemptPeriodMs) {
		checkArgument(maxNumberRestartAttempts >= 0, "Maximum number of restart attempts must be at least 0.");
		checkArgument(backoffTimeMS >= 0, "Backoff time between restart attempts must be at least 0 ms.");

		this.maxNumberRestartAttempts = maxNumberRestartAttempts;
		this.backoffTimeMS = backoffTimeMS;
		this.currentRestartAttempt = new AtomicInteger(0);
		this.strategyString = generateStrategyString();
		this.resetRestartAttemptPeriodMs = resetRestartAttemptPeriodMs;

		if (resetRestartAttemptPeriodMs > 0) {
			resetCurrentRestartAttemptSchedule();
		}
	}

	public void resetCurrentRestartAttemptSchedule() {
		scheduleExecutor.scheduleAtFixedRate(() -> {
			currentRestartAttempt.set(0);
			LOG.info("Reset task restart attempt value to zero.");
		}, 0, resetRestartAttemptPeriodMs, TimeUnit.MILLISECONDS);
	}

	public static FixedDelayRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		int maxAttempts = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
		long delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY).toMillis();
		long period = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_RESET_ATTEMPTS_PERIOD).toMillis();

		return new FixedDelayRestartBackoffTimeStrategyFactory(maxAttempts, delay, period);
	}

	@Override
	public boolean canRestart() {
		return currentRestartAttempt.get() <= maxNumberRestartAttempts;
	}

	@Override
	public long getBackoffTime() {
		return backoffTimeMS;
	}

	public int getMaxNumberRestartAttempts() {
		return maxNumberRestartAttempts;
	}

	@Override
	public void notifyFailure(Throwable cause) {
		currentRestartAttempt.incrementAndGet();
		LOG.info("Task failed. Restart attempt {} of {}.", currentRestartAttempt.get(), maxNumberRestartAttempts);
	}

	@Override
	public String toString() {
		return strategyString;
	}

	private String generateStrategyString() {
		StringBuilder str = new StringBuilder("FixedDelayRestartBackoffTimeStrategy(");
		str.append("maxNumberRestartAttempts=");
		str.append(maxNumberRestartAttempts);
		str.append(", backoffTimeMS=");
		str.append(backoffTimeMS);
		str.append(")");

		return str.toString();
	}

	/**
	 * The factory for creating {@link FixedDelayRestartBackoffTimeStrategy}.
	 */
	public static class FixedDelayRestartBackoffTimeStrategyFactory implements RestartBackoffTimeStrategy.Factory {

		private final int maxNumberRestartAttempts;

		private final long backoffTimeMS;

		private final long resetRestartAttemptPeriodMs;

		public FixedDelayRestartBackoffTimeStrategyFactory(int maxNumberRestartAttempts, long backoffTimeMS) {
			this(maxNumberRestartAttempts, backoffTimeMS, DISABLE_RESETTING_PERIOD);
		}

		public FixedDelayRestartBackoffTimeStrategyFactory(
				int maxNumberRestartAttempts, long backoffTimeMS, long resetRestartAttemptPeriodMs) {
			this.maxNumberRestartAttempts = maxNumberRestartAttempts;
			this.backoffTimeMS = backoffTimeMS;
			this.resetRestartAttemptPeriodMs = resetRestartAttemptPeriodMs;
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new FixedDelayRestartBackoffTimeStrategy(
					maxNumberRestartAttempts,
					backoffTimeMS,
					resetRestartAttemptPeriodMs);
		}
	}
}
