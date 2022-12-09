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

package org.apache.flink.runtime.taskmanager;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A report about the current states of all task execution of the TaskExecutor.
 */
public class TaskExecutionStateReport implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Collection<TaskExecutionState> taskExecutionStates;

	public TaskExecutionStateReport() {
		this(Collections.emptyList());
	}

	public TaskExecutionStateReport(TaskExecutionState taskExecutionState) {
		this(Collections.singletonList(taskExecutionState));
	}

	public TaskExecutionStateReport(final Collection<TaskExecutionState> taskExecutionStates) {
		this.taskExecutionStates = checkNotNull(taskExecutionStates);
	}

	public Collection<TaskExecutionState> getTaskExecutionStates() {
		return taskExecutionStates;
	}
}
