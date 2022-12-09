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

package org.apache.flink.runtime.statistics;

import java.util.Map;

/**
 * Interface for reporters that actively send out data after manual invocation.
 */
public interface Sender {

	/**
	 * Send flink statistics data. This method can be called by other components
	 * to send data for once.
	 *
	 * @param name statistics data's name
	 * @param value statistics data's value
	 * @param dimensions K-V pairs which represent features of the statistics data
	 */
	void send(String name, String value, Map<String, String> dimensions);

	/**
	 * Report started event tracking. This method can be called by other components.
	 * @param phase the phase of the job
	 */
	void sendStartTracking(StartupUtils.StartupPhase phase, long timestamp);

	/**
	 * Report finished event tracking. This method can be called by other components.
	 * @param phase the phase of the job
	 * @param isSucceeded whether the event is succeeded
	 * @param dimensions K-V pairs which represent features of the statistics data
	 */
	void sendEndTracking(StartupUtils.StartupPhase phase, boolean isSucceeded, Map<String, String> dimensions);

	default void send(String name, long value, Map<String, String> dimensions) {
		send(name, String.valueOf(value), dimensions);
	}

}
