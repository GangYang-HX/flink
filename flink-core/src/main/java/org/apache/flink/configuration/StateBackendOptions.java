/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

/** A collection of all configuration options that relate to state backend. */
public class StateBackendOptions {

	// ------------------------------------------------------------------------
	//  general state backend options
	// ------------------------------------------------------------------------

	/**
	 * The checkpoint storage used to store operator state locally within the cluster during
	 * execution.
	 *
	 * <p>The implementation can be specified either via their shortcut name, or via the class name
	 * of a {@code StateBackendFactory}. If a StateBackendFactory class name is specified, the
	 * factory is instantiated (via its zero-argument constructor) and its {@code
	 * StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
	 *
	 * <p>Recognized shortcut names are 'hashmap' and 'rocksdb'.
	 */
	public static final ConfigOption<Integer> ROCKSDB_METRIC_SAMPLE_RATE =
		ConfigOptions.key("state.backend.rocksdb.metric.read-or-not-sample-rate")
			.intType()
			.defaultValue(30)
			.withDescription("The rocksdb's sample rate when catching the metrics of get, seek and next.");

	public static final ConfigOption<Boolean> ROCKSDB_INTERFACE_COMPRESS_VALUE_ENABLED =
			ConfigOptions.key("state.backend.rocksdb.interface-compress-value.enabled")
					.booleanType()
					.noDefaultValue()
					.withDescription("This config can decide whether to conmpress the value of rocksdb.");
}
