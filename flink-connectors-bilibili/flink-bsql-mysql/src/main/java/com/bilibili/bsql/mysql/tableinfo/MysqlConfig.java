package com.bilibili.bsql.mysql.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/29 9:13 下午
 */
public class MysqlConfig {
    public static final String IDLE_CONNECTION_TEST_PERIOD = "idleConnectionTestPeriod";
    public static final String MAX_IDLE_TIME = "maxIdleTime";
    public static final String TM_CONNECTION_POOL_SIZE = "tmConnectionPoolSize";
    public static final String ENABLE_COMPRESSION = "enableCompression";
	public static final String ENABLE_DELETE = "enableDelete";

    // side
    public static final ConfigOption<Integer> BSQL_IDLE_CONNECTION_TEST_PERIOD_TYPE = ConfigOptions
            .key(IDLE_CONNECTION_TEST_PERIOD)
            .intType()
            .defaultValue(18000)
            .withDescription("c3p0 idleConnectionTestPeriod config.");

    public static final ConfigOption<Integer> BSQL_MAX_IDLE_TIME = ConfigOptions
            .key(MAX_IDLE_TIME)
            .intType()
            .defaultValue(1800)
            .withDescription("c3p0 maxIdleTime config");


    public static final ConfigOption<Integer> BSQL_TM_CONNECTION_POOL_SIZE = ConfigOptions
            .key(TM_CONNECTION_POOL_SIZE)
            .intType()
            .defaultValue(3)
            .withDescription("Task manager Connection pool size");

    // sink
    public static final ConfigOption<Boolean> BSQL_SINK_COMPRESS = ConfigOptions
            .key(ENABLE_COMPRESSION)
            .booleanType()
            .defaultValue(false)
            .withDescription("whether enable compression");

	public static final ConfigOption<Boolean> BSQL_SINK_DELETE = ConfigOptions
		.key(ENABLE_DELETE)
		.booleanType()
		.defaultValue(false)
		.withDescription("whether enable delete");
}
