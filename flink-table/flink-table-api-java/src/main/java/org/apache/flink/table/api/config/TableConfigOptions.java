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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.SqlDialect;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by
 * table planner.
 *
 * <p>NOTE: All option keys in this class must start with "table".
 */
@PublicEvolving
public class TableConfigOptions {
	private TableConfigOptions() {}

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED =
			key("table.dynamic-table-options.enabled")
					.booleanType()
					.defaultValue(false)
					.withDescription("Enable or disable the OPTIONS hint used to specify table options" +
							"dynamically, if disabled, an exception would be thrown " +
							"if any OPTIONS hint is specified");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> TABLE_SQL_DIALECT = key("table.sql-dialect")
			.stringType()
			.defaultValue(SqlDialect.DEFAULT.name().toLowerCase())
			.withDescription("The SQL dialect defines how to parse a SQL query. " +
					"A different SQL dialect may support different SQL grammar. " +
					"Currently supported dialects are: default and hive");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_REUSE_USER_DEFINE_FUNCTION_ENABLED =
		key("table.optimizer.reuse-user-defined-function.enabled")
			.booleanType()
			.defaultValue(true)
			.withDescription("Bsql reuse user defined function in codegen.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_PRE_COMPILE_CALC_CODEGEN_ENABLED =
		key("table.optimizer.pre-compile-calc-codegen.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("pre compile CalCodeGenerator result.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_UDF_METRIC_OPENED =
		key("table.optimizer.udf-metric.opened")
			.booleanType()
			.defaultValue(false)
			.withDescription("Open udf metric. default is closed.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> LOCAL_TIME_ZONE =
			key("table.local-time-zone")
					.stringType()
					// special value to decide whether to use ZoneId.systemDefault() in
					// TableConfig.getLocalTimeZone()
					.defaultValue("default")
					.withDescription(
							"The local time zone defines current session time zone id. It is used when converting to/from "
									+ "<code>TIMESTAMP WITH LOCAL TIME ZONE</code>. Internally, timestamps with local time zone are always represented in the UTC time zone. "
									+ "However, when converting to data types that don't include a time zone (e.g. TIMESTAMP, TIME, or simply STRING), "
									+ "the session time zone is used during conversion. The input of option is either a full name "
									+ "such as \"America/Los_Angeles\", or a custom timezone id such as \"GMT-08:00\".");

}
