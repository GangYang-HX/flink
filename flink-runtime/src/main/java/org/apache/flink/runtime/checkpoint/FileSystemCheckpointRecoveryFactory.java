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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FileSystemCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

	/** The FileSystem to use. */
	private final FileSystem fs;

	private final Configuration config;

	private final Executor executor;

	public FileSystemCheckpointRecoveryFactory(
			FileSystem fs,
			Configuration config,
			Executor executor) {
		this.fs = checkNotNull(fs, "FileSystem");
		this.config = checkNotNull(config, "Configuration");
		this.executor = checkNotNull(executor, "Executor");
	}

	@Override
	public CompletedCheckpointStore createCheckpointStore(
			JobID jobId,
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader) throws Exception {
		return new FileSystemCompletedCheckpointStore(fs, config, jobId, executor);
	}

	@Override
	public CheckpointIDCounter createCheckpointIDCounter(JobID jobID) throws Exception {
		return new FileSystemCheckpointIDCounter(fs, config, jobID);
	}

}
