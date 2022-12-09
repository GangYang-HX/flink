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

package org.apache.flink.bili.writer;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by filesystem(Including hive) connector.
 */
public class FileSystemOptions {

	public static final ConfigOption<String> PARTITION_SPLIT_KIND =
		key("partition.spliter.kind")
			.defaultValue("default")
			.withDescription("define how to split the path.");

	public static final ConfigOption<String> PARTITION_SPLIT_CLASS =
		key("partition.spliter.class")
			.noDefaultValue()
			.withDescription("The class can split the path.");

	public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_KIND =
			key("partition.time-extractor.kind")
					.defaultValue("default")
					.withDescription("Time extractor to extract time from partition values." +
							" Support default and custom." +
							" For default, can configure timestamp pattern." +
							" For custom, should configure extractor class.");

	public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_CLASS =
			key("partition.time-extractor.class")
					.noDefaultValue()
					.withDescription("The extractor class for implement PartitionTimeExtractor interface.");

	public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN =
			key("partition.time-extractor.timestamp-pattern")
					.noDefaultValue()
					.withDescription("The 'default' construction way allows users to use partition" +
							" fields to get a legal timestamp pattern." +
							" Default support 'yyyy-mm-dd hh:mm:ss' from first field." +
							" If timestamp in partition is single field 'dt', can configure: '$dt'." +
							" If timestamp in partition is year, month, day, hour," +
							" can configure: '$year-$month-$day $hour:00:00'." +
							" If timestamp in partition is dt and hour, can configure: '$dt $hour:00:00'.");

	//todo: make clear what is this problem
	public static final ConfigOption<String> SINK_PARTITION_COMMIT_TRIGGER =
			key("sink.partition-commit.trigger")
					.defaultValue("process-time")
					.withDescription("Trigger type for partition commit:" +
							" 'partition-time': extract time from partition," +
							" if 'watermark' > 'partition-time' + 'delay', will commit the partition." +
							" 'process-time': use processing time, if 'current processing time' > " +
							"'partition directory creation time' + 'delay', will commit the partition.");

	//millisecond
	public static final ConfigOption<Long> SINK_PARTITION_COMMIT_DELAY =
			key("sink.partition-commit.delay")
					.defaultValue(600000L)
					.withDescription("The partition will not commit until the delay time." +
							" if it is a day partition, should be '1 d'," +
							" if it is a hour partition, should be '1 h'");

    public static final ConfigOption<Boolean> SINK_PARTITION_COMMIT_EAGERLY =
            key("sink.partition-commit.eagerly")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to commit the partition as soon as possible");

	public static final ConfigOption<Boolean> SINK_PARTITION_ALLOW_DRIFT =
		key("sink.partition-allow.drift")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to allow the data drift to the latest partition");

	public static final ConfigOption<Boolean> SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY =
		key("sink.partition-path.contain.partitionkey")
			.booleanType()
			.defaultValue(true)
			.withDescription("Whether to allow partition path contain partition key");

	public static final ConfigOption<Boolean> SINK_PARTITION_ALLOW_EMPTY_COMMIT =
		key("sink.partition-allow.empty.commit")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to allow commit empty partition");

	public static final ConfigOption<Long> SINK_PARTITION_EMPTY_SWITCH_TIME =
			key("sink.partition-empty.switch.time")
			.defaultValue(0l)
			.withDescription("lzo switch orc timestamp");
	public static final ConfigOption<Boolean> SINK_PARTITION_ROLLBACK =
			key("sink.partition-partition.rollback")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to allow the partition rollback orc->text");

	public static final ConfigOption<Boolean> SINK_PARTITION_ENABLE_INDEX_FILE =
		key("sink.partition-enable.index.file")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to enable index file");

	public static final ConfigOption<String> SINK_PARTITION_TRACE_ID =
		key("sink.partition-traceId")
			.noDefaultValue()
			.withDescription("traceId");
	public static final ConfigOption<String> SINK_PARTITION_TRACE_KIND=
		key("sink.partition-traceKind")
			.noDefaultValue()
			.withDescription("traceKind");
	public static final ConfigOption<String> SINK_PARTITION_CUSTOM_TRACE_CLASS=
		key("sink.partition-custom.trace.class")
			.noDefaultValue()
			.withDescription("custom.trace.class");
	public static final ConfigOption<Integer> SINK_PARTITION_META_POS=
			key("sink.partition-meta.pos")
			.defaultValue(-1)
			.withDescription("meta.pos");
	public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_KIND =
			key("sink.partition-commit.policy.kind")
					.noDefaultValue()
					.withDescription("Policy to commit a partition is to notify the downstream" +
							" application that the partition has finished writing, the partition" +
							" is ready to be read." +
							" success-file: add '_success' file to directory."
							);

	public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_CLASS =
			key("sink.partition-commit.policy.class")
					.noDefaultValue()
					.withDescription("The partition commit policy class for implement" +
							" PartitionCommitPolicy interface. Only work in custom commit policy");

	public static final ConfigOption<String> SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME =
			key("sink.partition-commit.success-file.name")
					.defaultValue("_SUCCESS")
					.withDescription("The file name for success-file partition commit policy," +
							" default is '_SUCCESS'.");

	public static final ConfigOption<Long> SINK_ROLLING_POLICY_FILE_SIZE = key("sink.rolling-policy.file-size")
		.defaultValue(1024L * 1024L * 128L)
		.withDescription("The maximum part file size before rolling (by default 128MB).");

	public static final ConfigOption<Long> SINK_ROLLING_POLICY_TIME_INTERVAL = key("sink.rolling-policy.time.interval")
		.defaultValue(10L * 60 * 1000L)
		.withDescription("The maximum time duration a part file can stay open before rolling" +
			" (by default 10 min to avoid to many small files).");

	public static final ConfigOption<Integer> SINK_WRITER_PARALLEL = key("sink.writer.parallel")
		.defaultValue(1)
		.withDescription("the writer parallelism");

	public static final ConfigOption<String> SINK_HIVE_VERSION = key("sink.hive.version")
		.defaultValue("2.3.4")
		.withDescription("sink hive version.");
	public static final ConfigOption<String> SINK_HIVE_TABLE_FORMAT = key("sink.hive.table.format")
			.noDefaultValue()
			.withDescription("sink hive table format");
	public static final ConfigOption<String> SINK_HIVE_TABLE_COMPRESS = key("sink.hive.table.compress")
			.noDefaultValue()
			.withDescription("sink hive table compress");

	/**
	 *    caution: SINK_HIVE_DATABASE key value must equals with
	 *    "ArcherConstants.SINK_DATABASE_NAME_KEY" in flink-bilibili-external
	 */
	public static final ConfigOption<String> SINK_HIVE_DATABASE = key("sink.database.name")
		.noDefaultValue()
		.withDescription("sink hive database,");

	/**
	 *    caution: SINK_HIVE_TABLE key value must equals with
	 *    "ArcherConstants.SINK_TABLE_NAME_KEY" in flink-bilibili-external
	 */
	public static final ConfigOption<String> SINK_HIVE_TABLE = key("sink.table.name")
		.noDefaultValue()
		.withDescription("sink hive table");
	public static final ConfigOption<String> SYSTEM_USER_ID = key("hdfs.partition.commit.user")
		.defaultValue("")
		.withDescription("hdfs partition commit user,set conf by client");

	public static final ConfigOption<Integer> SINK_METASTORE_COMMIT_LIMIT_NUM = key("sink.metastore.commit.limit")
		.defaultValue(23)
		.withDeprecatedKeys("sink metastore commit limit num");


	public static final ConfigOption<String> SINK_PARTITION_OPTIMIZE_TYPE =
		key("sink.partition.optimize.type")
			.defaultValue("adaptive-partitioner")
			.withDescription("sink partition optimize type");

	public static final ConfigOption<Boolean> SINK_DEFAULT_PARTITION_CHECK = key("sink.default.partition.check")
		.defaultValue(false)
		.withDeprecatedKeys("default sink partition, when this config is true, commit will always execute without " +
			"checkout.");
	public static final ConfigOption<Boolean> SINK_MULTI_PARTITION_EMPTY_COMMIT = key("sink.multi.partition.empty.commit")
		.defaultValue(false)
		.withDeprecatedKeys("default sink partition, when this config is true, commit will always execute without " +
			"checkout.");
}
