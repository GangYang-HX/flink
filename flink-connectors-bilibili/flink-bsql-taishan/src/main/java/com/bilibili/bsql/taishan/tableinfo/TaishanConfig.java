package com.bilibili.bsql.taishan.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author zhuzhengjun
 * @date 2020/10/29 11:23 上午
 */
public class TaishanConfig {


    public static final String CLUSTER = "cluster";
    public static final String ZONE = "zone";
    public static final String TABLE = "table";
    public static final String PASSWORD = "password";

    public static final String TIMEOUT = "timeout";
    public static final String TTL = "ttl";
	public static final String RETRY_COUNT = "maxRetry";
	public static final String DURATION = "duration";
	public static final String KEEP_ALIVE_TIME = "keepAliveTime";
	public static final String KEEP_ALIVE_TIMEOUT = "keepAliveTimeOut";
	public static final String IDLE_TIMEOUT = "idleTimeOut";
	public static final String RETRY_INTERVAL = "retryInterval";

    public static final ConfigOption<String> BSQL_TAISHAN_CLUSTER = ConfigOptions
            .key(CLUSTER)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql taishan cluster");

    public static final ConfigOption<String> BSQL_TAISHAN_ZONE = ConfigOptions
            .key(ZONE)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql taishan zone");

    public static final ConfigOption<String> BSQL_TAISHAN_TABLE = ConfigOptions
            .key(TABLE)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql taishan table");

    public static final ConfigOption<String> BSQL_TAISHAN_PASSWORD = ConfigOptions
            .key(PASSWORD)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql taishan password");

    public static final ConfigOption<Integer> BSQL_TAISHAN_TTL = ConfigOptions
            .key(TTL)
            .intType()
            .noDefaultValue()
            .withDescription("bsql taishan ttl");

    public static final ConfigOption<String> BSQL_TAISHAN_TIMEOUT = ConfigOptions
            .key(TIMEOUT)
            .stringType()
            .defaultValue("60000")
            .withDescription("bsql taishan timeout, set default value 60s");

	public static final ConfigOption<Integer> BSQL_TAISHAN_RETRY_COUNT = ConfigOptions
		.key(RETRY_COUNT)
		.intType()
		.defaultValue(3)
		.withDescription("bsql taishan max retry times");

	public static final ConfigOption<Integer> BSQL_TAISHAN_DURATION = ConfigOptions
		.key(DURATION)
		.intType()
		.defaultValue(2)
		.withDescription("bsql taishan duration, grpc timeout");

	public static final ConfigOption<Integer> BSQL_TAISHAN_KEEP_ALIVE_TIME = ConfigOptions
		.key(KEEP_ALIVE_TIME)
		.intType()
		.defaultValue(1)
		.withDescription("bsql taishan duration, value as seconds");

	public static final ConfigOption<Integer> BSQL_TAISHAN_KEEP_ALIVE_TIMEOUT = ConfigOptions
		.key(KEEP_ALIVE_TIMEOUT)
		.intType()
		.defaultValue(1000)
		.withDescription("bsql taishan duration, value as milliseconds");

	public static final ConfigOption<Integer> BSQL_TAISHAN_IDLE_TIMEOUT = ConfigOptions
		.key(IDLE_TIMEOUT)
		.intType()
		.defaultValue(1)
		.withDescription("bsql taishan duration, value as minutes");

	public static final ConfigOption<Integer> BSQL_TAISHAN_RETRY_INTERVAL = ConfigOptions
		.key(RETRY_INTERVAL)
		.intType()
		.defaultValue(100)
		.withDescription("bsql taishan duration, value as milliseconds");





}
