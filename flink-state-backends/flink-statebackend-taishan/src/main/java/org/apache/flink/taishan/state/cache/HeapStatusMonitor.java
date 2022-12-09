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

package org.apache.flink.taishan.state.cache;

import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitor memory usage and garbage collection.
 */
public class HeapStatusMonitor {

	private static final Logger LOG = LoggerFactory.getLogger(HeapStatusMonitor.class);

	private final MemoryMXBean memoryMXBean;

	private final long maxMemory;

	private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;

	/** Generate ascending id for each monitor result. */
	private final AtomicLong resultIdGenerator;

	/** Executor to check memory usage periodically. */

	/** Time for gc when last check. */
	private long lastGcTime;

	/** Number of gc when last check. */
	private long lastGcCount;

	/** Flag to signify that the monitor has been shut down already. */
	private final AtomicBoolean isShutdown = new AtomicBoolean();

	/** Shutdown hook to make sure that scheduler is closed. */


	HeapStatusMonitor() {
		this.memoryMXBean = ManagementFactory.getMemoryMXBean();
		this.maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
		this.garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
		this.resultIdGenerator = new AtomicLong(0L);
		this.lastGcTime = 0L;
		this.lastGcCount = 0L;

	}

	public MonitorResult runCheck() {
		long timestamp = System.currentTimeMillis();
		long id = resultIdGenerator.getAndIncrement();
		return new MonitorResult(timestamp, id, memoryMXBean.getHeapMemoryUsage(), getGarbageCollectionTime());
	}

	private long getGarbageCollectionTime() {
		long count = 0;
		long timeMillis = 0;
		for (GarbageCollectorMXBean gcBean : garbageCollectorMXBeans) {
			long c = gcBean.getCollectionCount();
			long t = gcBean.getCollectionTime();
			count += c;
			timeMillis += t;
		}

		if (count == lastGcCount) {
			return 0;
		}

		long gcCountIncrement = count - lastGcCount;
		long averageGcTime = (timeMillis - lastGcTime) / gcCountIncrement;

		lastGcCount = count;
		lastGcTime = timeMillis;

		return averageGcTime;
	}

	public long getMaxMemory() {
		return maxMemory;
	}

	/**
	 * Monitor result.
	 */
	static class MonitorResult {

		/** Time of status. */
		private final long timestamp;

		/** Unique id of status. */
		private final long id;

		private final long totalMemory;

		private final long totalUsedMemory;

		private final long garbageCollectionTime;

		MonitorResult(long timestamp, long id, MemoryUsage memoryUsage, long garbageCollectionTime) {
			this(timestamp, id, memoryUsage.getMax(), memoryUsage.getUsed(), garbageCollectionTime);
		}

		MonitorResult(long timestamp, long id, long totalMemory, long totalUsedMemory, long garbageCollectionTime) {
			this.timestamp = timestamp;
			this.id = id;
			this.totalMemory = totalMemory;
			this.totalUsedMemory = totalUsedMemory;
			this.garbageCollectionTime = garbageCollectionTime;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getId() {
			return id;
		}

		public long getTotalMemory() {
			return totalMemory;
		}

		public long getTotalUsedMemory() {
			return totalUsedMemory;
		}

		public long getGarbageCollectionTime() {
			return garbageCollectionTime;
		}

		@Override
		public String toString() {
			return "MonitorResult{" +
				"timestamp=" + timestamp +
				", id=" + id +
				", totalMemory=" + totalMemory +
				", totalUsedMemory=" + totalUsedMemory +
				", garbageCollectionTime=" + garbageCollectionTime +
				'}';
		}
	}
}
