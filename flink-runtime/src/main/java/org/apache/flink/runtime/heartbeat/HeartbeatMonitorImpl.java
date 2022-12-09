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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default implementation of {@link HeartbeatMonitor}.
 *
 * @param <O> Type of the payload being sent to the associated heartbeat target
 */
public class HeartbeatMonitorImpl<O> implements HeartbeatMonitor<O>, Runnable {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/** Resource ID of the monitored heartbeat target. */
	private final ResourceID resourceID;

	/** Associated heartbeat target. */
	private final HeartbeatTarget<O> heartbeatTarget;

	private final ScheduledExecutor scheduledExecutor;

	/** Listener which is notified about heartbeat timeouts. */
	private final HeartbeatListener<?, ?> heartbeatListener;

	private final boolean haReconcileEnabled;

	private final long yarnExpiryIntervalMs;

	/** Maximum heartbeat timeout interval. */
	private final long heartbeatTimeoutIntervalMs;

	private volatile ScheduledFuture<?> futureTimeout;

	private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

	private volatile long lastHeartbeat;

	private volatile boolean shouldWaitForNewJobManager = false;

	HeartbeatMonitorImpl(
		ResourceID resourceID,
		HeartbeatTarget<O> heartbeatTarget,
		ScheduledExecutor scheduledExecutor,
		HeartbeatListener<?, O> heartbeatListener,
		long heartbeatTimeoutIntervalMs,
		boolean haReconcileEnabled,
		long yarnExpiryIntervalMs) {

		this.resourceID = Preconditions.checkNotNull(resourceID);
		this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
		this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);
		this.haReconcileEnabled = haReconcileEnabled;
		this.yarnExpiryIntervalMs = yarnExpiryIntervalMs;

		Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout interval has to be larger than 0.");
		this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

		lastHeartbeat = 0L;

		resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
	}

	@Override
	public HeartbeatTarget<O> getHeartbeatTarget() {
		return heartbeatTarget;
	}

	@Override
	public ResourceID getHeartbeatTargetId() {
		return resourceID;
	}

	@Override
	public long getLastHeartbeat() {
		return lastHeartbeat;
	}

	@Override
	public void reportHeartbeat() {
		lastHeartbeat = System.currentTimeMillis();
		resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
	}

	@Override
	public void cancel() {
		// we can only cancel if we are in state running
		if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
			cancelTimeout();
		}
	}

	@Override
	public void run() {
		if (haReconcileEnabled && yarnExpiryIntervalMs > 0L && shouldWaitForNewJobManager) {
			waitForNewJobManager(yarnExpiryIntervalMs);
			return;
		}
		// The heartbeat has timed out if we're in state running
		if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
			heartbeatListener.notifyHeartbeatTimeout(resourceID);
		}
	}

	void waitForNewJobManager(long yarnExpiryIntervalMs) {
		if (state.get() == State.RUNNING) {
			// wait for yarnExpiryIntervalMs for yarn to start appMaster
			logger.warn("Wait for a more time ({} ms) for {}", yarnExpiryIntervalMs, heartbeatListener.getClass());
			futureTimeout = scheduledExecutor.schedule(this, yarnExpiryIntervalMs, TimeUnit.MILLISECONDS);

			// Double check for concurrent accesses (e.g. a firing of the scheduled future)
			if (state.get() != State.RUNNING) {
				cancelTimeout();
			}
			shouldWaitForNewJobManager = false;
		}
	}

	public boolean isCanceled() {
		return state.get() == State.CANCELED;
	}

	void resetHeartbeatTimeout(long heartbeatTimeout) {
		if (state.get() == State.RUNNING) {
			cancelTimeout();

			futureTimeout = scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

			// Double check for concurrent accesses (e.g. a firing of the scheduled future)
			if (state.get() != State.RUNNING) {
				cancelTimeout();
			}
			shouldWaitForNewJobManager = true;
		}
	}

	private void cancelTimeout() {
		if (futureTimeout != null) {
			futureTimeout.cancel(true);
		}
	}

	private enum State {
		RUNNING,
		TIMEOUT,
		CANCELED
	}

	/**
	 * The factory that instantiates {@link HeartbeatMonitorImpl}.
	 *
	 * @param <O> Type of the outgoing heartbeat payload
	 */
	static class Factory<O> implements HeartbeatMonitor.Factory<O> {

		@Override
		public HeartbeatMonitor<O> createHeartbeatMonitor(
			ResourceID resourceID,
			HeartbeatTarget<O> heartbeatTarget,
			ScheduledExecutor mainThreadExecutor,
			HeartbeatListener<?, O> heartbeatListener,
			long heartbeatTimeoutIntervalMs,
			boolean haReconcileEnabled,
			long yarnExpiryIntervalMs) {

			return new HeartbeatMonitorImpl<>(
				resourceID,
				heartbeatTarget,
				mainThreadExecutor,
				heartbeatListener,
				heartbeatTimeoutIntervalMs,
			 	haReconcileEnabled,
				yarnExpiryIntervalMs);
		}
	}
}
