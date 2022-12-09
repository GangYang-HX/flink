package com.bilibili.bsql.clickhouse.shard.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author: zhuzhengjun
 * @date: 2021/2/19 4:03 下午
 */
public class ClickHouseShardConfig {
	private static final String URL = "url";
	private static final String USERNAME = "userName";
	private static final String PASSWORD = "password";
	private static final String MAX_RETRIES = "maxRetries";
	private static final String PARTITION_STRATEGY = "partitionStrategy";
	public static final String DATABASE_NAME = "databaseName";
	public static final String TABLE_NAME = "tableName";
	public static final String CHARSET = "charset";
	public static final String SOCKET_TIMEOUT = "socketTimeout";
	public static final String CONNECTION_TIMEOUT = "connectionTimeout";



	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_URL = ConfigOptions
		.key(URL)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql clickhouse shard url");

	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_USERNAME = ConfigOptions
		.key(USERNAME)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql clickhouse shard username");

	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_PASSWORD = ConfigOptions
		.key(PASSWORD)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql clickhouse shard password");
	public static final ConfigOption<Integer> BSQL_CLICKHOUSE_SHARD_MAX_RETRIES = ConfigOptions
		.key(MAX_RETRIES)
		.intType()
		.defaultValue(3)
		.withDescription("bsql clickhouse shard maxRetries，default 3 times");

	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_PARTITION_STRATEGY = ConfigOptions
		.key(PARTITION_STRATEGY)
		.stringType()
		.defaultValue("balanced")
		.withDescription("bsql clickhouse shard partitionStrategy");
	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_DATABASE_NAME = ConfigOptions
		.key(DATABASE_NAME)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql clickhouse shard databaseName");
	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_TABLE_NAME = ConfigOptions
		.key(TABLE_NAME)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql clickhouse shard databaseName");

	public static final ConfigOption<String> BSQL_CLICKHOUSE_SHARD_CHARSET = ConfigOptions
		.key(CHARSET)
		.stringType()
		.defaultValue("utf8")
		.withDescription("bsql clickhouse shard charset");

	public static final ConfigOption<Long> BSQL_CLICKHOUSE_SHARD_SOCKET_TIMEOUT = ConfigOptions
		.key(SOCKET_TIMEOUT)
		.longType()
		.defaultValue(3000000L)
		.withDescription("bsql clickhouse shard charset socketTimeout");

	public static final ConfigOption<Long> BSQL_CLICKHOUSE_SHARD_CONNECTION_TIMEOUT = ConfigOptions
		.key(CONNECTION_TIMEOUT)
		.longType()
		.defaultValue(3000000L)
		.withDescription("bsql clickhouse shard charset connectionTimeout");








}
