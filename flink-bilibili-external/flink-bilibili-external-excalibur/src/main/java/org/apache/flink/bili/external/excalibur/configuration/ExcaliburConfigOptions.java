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

package org.apache.flink.bili.external.excalibur.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class ExcaliburConfigOptions {

	public static final ConfigOption<Double> VCORES =
		key("yarn.containers.vcores")
			.doubleType()
			.defaultValue(-1.0)
			.withDescription(Description.builder().text(
					"The number of virtual cores (vcores) per YARN container. By default, the number of vcores" +
						" is set to the number of slots per TaskManager, if set, or to 1, otherwise. In order for this" +
						" parameter to be used your cluster must have CPU scheduling enabled. You can do this by setting" +
						" the %s.",
					code("org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"))
				.build());

	public static final ConfigOption<String> APPLICATION_QUEUE =
		key("yarn.application.queue")
			.stringType()
			.noDefaultValue()
			.withDescription("The YARN queue on which to put the current pipeline.");

	public static final ConfigOption<String> APPLICATION_NAME =
		key("yarn.application.name")
			.stringType()
			.noDefaultValue()
			.withDescription("A custom name for your YARN application.");

	public static final ConfigOption<String> YARN_CLUSTER =
		key("yarn.cluster")
			.stringType()
			.noDefaultValue()
			.withDescription("cluster: JSSZ、JSCS、etc.");

	public static final ConfigOption<String> FLINK_VERSION =
		key("flink.version")
			.stringType()
			.noDefaultValue()
			.withDescription("flink version, such as FLINK-1.11.4");

	public static final ConfigOption<String> PRIORITY =
		key("job.priority")
			.stringType()
			.defaultValue("P2")
			.withDescription("job priority");

	public static final ConfigOption<Long> METADATA_REPORT_FREQUENCY =
		key("metadata.report.frequency")
			.longType()
			.defaultValue(5L)
			.withDescription("report frequency");

	public static final ConfigOption<String> SESSION_CUSTOM_EXTRA_CONFIG =
		key("excalibur.session.custom.extra.config")
			.stringType()
			.defaultValue("")
			.withDescription("user custom session extra config.");

	public static final ConfigOption<String> JOB_CUSTOM_EXTRA_CONFIG =
		key("excalibur.job.custom.extra.config")
			.stringType()
			.defaultValue("")
			.withDescription("user custom job extra config.");
}
