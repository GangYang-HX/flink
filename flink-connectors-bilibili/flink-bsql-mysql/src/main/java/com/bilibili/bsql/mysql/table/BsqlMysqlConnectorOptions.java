package com.bilibili.bsql.mysql.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Bsql Mysql Connector Options. */
public class BsqlMysqlConnectorOptions {

    public static final String IDLE_CONNECTION_TEST_PERIOD = "idleConnectionTestPeriod";
    public static final String MAX_IDLE_TIME = "maxIdleTime";
    public static final String TM_CONNECTION_POOL_SIZE = "tmConnectionPoolSize";

    // -----------------------------------------------------------------------------------------
    // Lookup options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> BSQL_IDLE_CONNECTION_TEST_PERIOD_TYPE =
            ConfigOptions.key(IDLE_CONNECTION_TEST_PERIOD)
                    .intType()
                    .defaultValue(18000)
                    .withDescription("c3p0 idleConnectionTestPeriod config.");

    public static final ConfigOption<Integer> BSQL_MAX_IDLE_TIME =
            ConfigOptions.key(MAX_IDLE_TIME)
                    .intType()
                    .defaultValue(1800)
                    .withDescription("c3p0 maxIdleTime config");

    public static final ConfigOption<Integer> BSQL_TM_CONNECTION_POOL_SIZE =
            ConfigOptions.key(TM_CONNECTION_POOL_SIZE)
                    .intType()
                    .defaultValue(3)
                    .withDescription("Task manager Connection pool size");
}
