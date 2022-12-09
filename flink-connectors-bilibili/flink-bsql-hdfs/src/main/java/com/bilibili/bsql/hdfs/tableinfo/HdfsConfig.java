package com.bilibili.bsql.hdfs.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static com.bilibili.bsql.common.keys.TableInfoKeys.DELIMITER_KEY;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HdfsConfig.java
 * @description This is the description of HdfsConfig.java
 * @createTime 2020-10-22 21:36:00
 */
public class HdfsConfig {
    private static final String PATH = "path";
    private static final String PERIOD = "period";
    public static final String CONF = "conf";
    public static final String USER = "user";
    public static final String FETCH_SIZE = "fetchSize";
    public static final String FORMAT = "format";
    public static final String DELIMITER_KEY = "bsql-delimit.delimiterKey";
    public static final String MEMORY_THRESHOLD_SIZE = "memoryThresholdSize";
    public static final String FAILOVER_TIMES = "failoverTimes";
	public static final String CHECK_SUCCESS = "checkSuccess";

    public static final ConfigOption<String> BSQL_HDFS_PATH = ConfigOptions
            .key(PATH)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql hdfs path");

    public static final ConfigOption<String> BSQL_HDFS_PERIOD = ConfigOptions
            .key(PERIOD)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql hdfs period");

    public static final ConfigOption<String> BSQL_HDFS_DELIMITER_KEY = ConfigOptions
            .key(DELIMITER_KEY)
            .stringType()
            .defaultValue("|")
            .withDescription("bsql delimiter key");

    public static final ConfigOption<String> BSQL_HDFS_CONF = ConfigOptions
            .key(CONF)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql hdfs conf");

    public static final ConfigOption<String> BSQL_HDFS_USER = ConfigOptions
            .key(USER)
            .stringType()
            .defaultValue("hdfs")
            .withDescription("bsql hdfs user");

    public static final ConfigOption<Integer> BSQL_HDFS_FETCH_SIZE = ConfigOptions
            .key(FETCH_SIZE)
            .intType()
            .defaultValue(2000)
            .withDescription("bsql fetch size");

    public static final ConfigOption<String> BSQL_FORMAT = ConfigOptions
            .key(FORMAT)
            .stringType()
            .defaultValue("bsql-delimit")
            .withDescription("add a option to enable read format");

    public static final ConfigOption<Long> BSQL_MEMORY_THRESHOLD_SIZE = ConfigOptions
			.key(MEMORY_THRESHOLD_SIZE)
			.longType()
			.defaultValue(1024 * 1024 * 1024L)
			.withDescription("memory threshold size");

	public static final ConfigOption<Integer> BSQL_FAILOVER_TIMES = ConfigOptions
			.key(FAILOVER_TIMES)
			.intType()
			.defaultValue(1)
			.withDescription("failover times");

	public static final ConfigOption<String> BSQL_HDFS_CHECK_SUCCESS = ConfigOptions
		.key(CHECK_SUCCESS)
		.stringType()
		.defaultValue("false")
		.withDescription("bsql hdfs check success");
}
