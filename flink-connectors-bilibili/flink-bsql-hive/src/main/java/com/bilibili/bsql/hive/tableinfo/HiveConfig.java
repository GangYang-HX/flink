package com.bilibili.bsql.hive.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HiveConfig.java
 * @description This is the description of HiveConfig.java
 * @createTime 2020-10-28 16:20:00
 */
public class HiveConfig {
    private static final String TABLE_NAME = "tableName";
    private static final String LOCATION = "location";
    private static final String FIELD_DELIM = "fieldDelim";
    private static final String ROW_DELIM = "rowDelim";
    private static final String COMPRESS = "compress";
    private static final String BUFFER_SIZE = "bufferSize";
    private static final String BATCH_SIZE = "batchSize";
	private static final String ORC_ROWS_BETWEEN_MEMORY_CHECKS = "orcRowsBetweenMemoryChecks";
    private static final String FORMAT = "format";
    private static final String PARTITION_KEY = "partitionKey";
    private static final String TIME_FIELD = "timeField";
	private static final String TIME_FIELD_VIRTUAL = "timeFieldVirtual";
    private static final String META_FIELD = "metaField";
    private static final String BUCKET_TIME_KEY = "bucketTimeKey";
    private static final String HIVE_VERSION = "hiveVersion";
	private static final String HIVE_PARTITION_UDF = "partition.udf";
	private static final String HIVE_PARTITION_UDF_CLASS = "partition.udf.class";
    private static final String EAGERLY_COMMIT = "eagerlyCommit";
	private static final String ALLOW_DRIFT = "allowDrift";
	private static final String ALLOW_EMPTY_COMMIT  = "allowEmptyCommit";
	private static final String PATH_CONTAIN_PARTITION_KEY  = "pathContainPartitionKey";
	private static final String ENABLE_INDEX_FILE  = "enableIndexFile";
	private static final String TRACE_ID = "traceId";
	private static final String TRACE_KIND = "traceKind";
	private static final String COMMIT_POLICY = "commitPolicy";
	private static final String CUSTOM_POLICY_CLASS = "customPolicyClass";
	private static final String CUSTOM_TRACE_CLASS = "customTraceClass";
	private static final String METASTORE_COMMIT_LIMIT = "metastoreCommitLimit";
	private static final String TABLE_META_UDF = "table.meta.udf";
	private static final String TABLE_META_UDF_CLASS = "table.meta.udf.class";
	private static final String SPLIT_POLICY_UDF = "split.policy.udf";
	private static final String SPLIT_POLICY_UDF_CLASS = "split.policy.udf.class";
	private static final String TABLE_TAG_UDF = "table.tag.udf";
	private static final String TABLE_TAG_UDF_CLASS = "table.tag.udf.class";
	private static final String DEFAULT_PARTITION_CHECK = "default.partition.check";
	private static final String MULTI_PARTITION_EMPTY_COMMIT = "emptyCommit";
	private static final String HIVE_SWITCH_TIME = "switchTime";
	private static final String HIVE_PARTITION_ROLLBACK = "partitionRollback";
	private static final String HIVE_USE_TIMESTAMP_FILE_PREFIX = "useTimestampFilePrefix";
	private static final String SINK_ROLLING_POLICY_FILE_SIZE = "rollingPolicyFileSize";
	private static final String SINK_ROLLING_POLICY_TIME_INTERVAL = "rollingPolicyTimeInterval";
	private static final String HIVE_COMMIT_DELAY_MS = "commit.delay.ms";

    public static final ConfigOption<String> BSQL_HIVE_TABLE_NAME = ConfigOptions
            .key(TABLE_NAME)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table name");

    public static final ConfigOption<String> BSQL_HIVE_LOCATION = ConfigOptions
            .key(LOCATION)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table location");

    public static final ConfigOption<String> BSQL_HIVE_FIELD_DELIM = ConfigOptions
            .key(FIELD_DELIM)
            .stringType()
            .defaultValue("|")
            .withDescription("hive table fieldDelim");

    public static final ConfigOption<String> BSQL_HIVE_ROW_DELIM = ConfigOptions
            .key(ROW_DELIM)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table rowDelim");

    public static final ConfigOption<String> BSQL_HIVE_COMPRESS = ConfigOptions
            .key(COMPRESS)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table compress");

    public static final ConfigOption<String> BSQL_HIVE_BUFFER_SIZE = ConfigOptions
            .key(BUFFER_SIZE)
            .stringType()
            .noDefaultValue()
            .withDescription("buffer size");

    public static final ConfigOption<String> BSQL_HIVE_BATCH_SIZE = ConfigOptions
            .key(BATCH_SIZE)
            .stringType()
            .noDefaultValue()
            .withDescription("batch size");

    public static final ConfigOption<String> BSQL_HIVE_FORMAT = ConfigOptions
            .key(FORMAT)
            .stringType()
            .defaultValue("text")
            .withDescription("hive table format");

    public static final ConfigOption<String> BSQL_HIVE_PARTITION_KEY = ConfigOptions
            .key(PARTITION_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table partitionKey");

    public static final ConfigOption<Boolean> BSQL_HIVE_EAGERLY_COMMIT = ConfigOptions
            .key(EAGERLY_COMMIT)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to commit the partition as soon as possible");

    public static final ConfigOption<Boolean> BSQL_HIVE_ALLOW_DRIFT = ConfigOptions
            .key(ALLOW_DRIFT)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to allow the data drift to the latest partition");

    public static final ConfigOption<Boolean> BSQL_HIVE_ALLOW_EMPTY_COMMIT = ConfigOptions
            .key(ALLOW_EMPTY_COMMIT)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to allow commit empty partition");

    public static final ConfigOption<String> BSQL_HIVE_SWITCH_TIME = ConfigOptions
            .key(HIVE_SWITCH_TIME)
            .stringType()
            .noDefaultValue()
            .withDescription("lzo switch orc timestamp");
	public static final ConfigOption<Boolean> BSQL_HIVE_PARTITION_ROLLBACK = ConfigOptions
			.key(HIVE_PARTITION_ROLLBACK)
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to allow the partition rollback orc -> text");

    public static final ConfigOption<Boolean> BSQL_HIVE_PATH_CONTAIN_PARTITION_KEY = ConfigOptions
            .key(PATH_CONTAIN_PARTITION_KEY)
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to allow partition path contain partition key");

    public static final ConfigOption<Boolean> BSQL_HIVE_ENABLE_INDEX_FILE = ConfigOptions
            .key(ENABLE_INDEX_FILE)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to enable index file");

    public static final ConfigOption<String> BSQL_HIVE_TRACE_ID = ConfigOptions
            .key(TRACE_ID)
            .stringType()
            .noDefaultValue()
            .withDescription("trace id");
    public static final ConfigOption<String> BSQL_HIVE_TRACE_KIND = ConfigOptions
            .key(TRACE_KIND)
            .stringType()
            .noDefaultValue()
            .withDescription("trace kind");
    public static final ConfigOption<String> BSQL_CUSTOM_TRACE_CLASS = ConfigOptions
            .key(CUSTOM_TRACE_CLASS)
            .stringType()
            .defaultValue("org.apache.flink.bili.external.trace.LancerTrace")
            .withDeprecatedKeys("custom trace class reference in calsspath, default is lancer trace classs");
    public static final ConfigOption<String> BSQL_HIVE_TIME_FIELD = ConfigOptions
            .key(TIME_FIELD)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table timeField");
	public static final ConfigOption<String> BSQL_HIVE_META_FIELD = ConfigOptions
            .key(META_FIELD)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table metaField");
	public static final ConfigOption<String> BSQL_HIVE_BUCKET_TIME_KEY = ConfigOptions
            .key(BUCKET_TIME_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("hive table bucketTimeKey in metaField");
    public static final ConfigOption<Boolean> BSQL_HIVE_TIME_FIELD_VIRTUAL = ConfigOptions
            .key(TIME_FIELD_VIRTUAL)
            .booleanType()
            .defaultValue(false)
            .withDescription("is timeField virtual");
    public static final ConfigOption<String> BSQL_HIVE_VERSION = ConfigOptions
            .key(HIVE_VERSION)
            .stringType()
            .defaultValue("2.3.4")
            .withDescription("hive version.");
	public static final ConfigOption<String> BSQL_HIVE_PARTITION_UDF = ConfigOptions
		.key(HIVE_PARTITION_UDF)
		.stringType()
		.noDefaultValue()
		.withDescription("in order to map the partition k v");
	public static final ConfigOption<String> BSQL_HIVE_PARTITION_UDF_CLASS = ConfigOptions
		.key(HIVE_PARTITION_UDF_CLASS)
		.stringType()
		.noDefaultValue()
		.withDescription("partition udf main class");

    public static final ConfigOption<String> BSQL_COMMIT_POLICY = ConfigOptions
            .key(COMMIT_POLICY)
            .stringType()
            .defaultValue("metastore,custom")
            .withDeprecatedKeys("hive sink commit policy, default is metastore and archer");
    public static final ConfigOption<String> BSQL_CUSTOM_POLICY_CLASS = ConfigOptions
            .key(CUSTOM_POLICY_CLASS)
            .stringType()
            .defaultValue("org.apache.flink.bili.external.archer.policy.ArcherCommitPolicy")
            .withDeprecatedKeys("custom policy class reference in calsspath, default is archer commit policy class," +
                "if more than one , you can split it by ','");
	public static final ConfigOption<String> BSQL_METASTORE_COMMIT_LIMIT = ConfigOptions
		.key(METASTORE_COMMIT_LIMIT)
		.stringType()
		.defaultValue("23")
		.withDeprecatedKeys("commit policy limit num for protecting metastore server.");
	public static final ConfigOption<String> BSQL_TABLE_META_UDF = ConfigOptions
		.key(TABLE_META_UDF)
		.stringType()
		.noDefaultValue()
		.withDescription("table meta udf");
	public static final ConfigOption<String> BSQL_TABLE_META_UDF_CLASS = ConfigOptions
		.key(TABLE_META_UDF_CLASS)
		.stringType()
		.noDefaultValue()
		.withDescription("table meta udf class");
	public static final ConfigOption<String> BSQL_SPLIT_POLICY_UDF = ConfigOptions
		.key(SPLIT_POLICY_UDF)
		.stringType()
		.noDefaultValue()
		.withDescription("assert a record belongs to the table udf");
	public static final ConfigOption<String> BSQL_SPLIT_POLICY_UDF_CLASS = ConfigOptions
		.key(SPLIT_POLICY_UDF_CLASS)
		.stringType()
		.noDefaultValue()
		.withDescription("assert a record belongs to the table udf class");
	public static final ConfigOption<String> BSQL_TABLE_TAG_UDF = ConfigOptions
		.key(TABLE_TAG_UDF)
		.stringType()
		.noDefaultValue()
		.withDescription("table tag udf");
	public static final ConfigOption<String> BSQL_TABLE_TAG_UDF_CLASS = ConfigOptions
		.key(TABLE_TAG_UDF_CLASS)
		.stringType()
		.noDefaultValue()
		.withDescription("table tag udf class");
	public static final ConfigOption<Boolean> BSQL_DEFAULT_PARTITION_CHECK = ConfigOptions.
		key(DEFAULT_PARTITION_CHECK)
		.booleanType()
		.defaultValue(false)
		.withDeprecatedKeys("it will check the open/commit partition num when this config is open");

	public static final ConfigOption<Boolean> BSQL_HIVE_USE_TIMESTAMP_FILE_PREFIX = ConfigOptions
			.key(HIVE_USE_TIMESTAMP_FILE_PREFIX)
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to use timestamp partprefix");
	public static final ConfigOption<Boolean> BSQL_MULTI_PARTITION_EMPTY_COMMIT = ConfigOptions
		.key(MULTI_PARTITION_EMPTY_COMMIT)
		.booleanType()
		.defaultValue(false)
		.withDeprecatedKeys("it will reset commit time to system current time millions when cutoff is true");
	public static final ConfigOption<String> BSQL_ORC_ROWS_BETWEEN_MEMORY_CHECKS = ConfigOptions
			.key(ORC_ROWS_BETWEEN_MEMORY_CHECKS)
			.stringType()
			.noDefaultValue()
			.withDeprecatedKeys("orc.rows.between.memory.checks");
	public static final ConfigOption<String> BSQL_HIVE_COMMIT_DELAY_MS = ConfigOptions
		.key(HIVE_COMMIT_DELAY_MS)
		.stringType()
		.defaultValue("600000");
	public static final ConfigOption<Long> BSQL_SINK_ROLLING_POLICY_FILE_SIZE = ConfigOptions
		.key(SINK_ROLLING_POLICY_FILE_SIZE)
		.longType()
		.defaultValue(1024L * 1024L * 128L)
		.withDescription("The maximum part file size before rolling");
	public static final ConfigOption<Long> BSQL_SINK_ROLLING_POLICY_TIME_INTERVAL = ConfigOptions
		.key(SINK_ROLLING_POLICY_TIME_INTERVAL)
		.longType()
		.defaultValue(10L * 60 * 1000L)
		.withDescription("The maximum time duration a part file can stay open before rolling");


}
