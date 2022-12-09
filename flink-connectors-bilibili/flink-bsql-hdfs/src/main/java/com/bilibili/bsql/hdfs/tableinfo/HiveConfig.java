package com.bilibili.bsql.hdfs.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @Author: JinZhengyu
 * @Date: 2022/7/28 上午10:23
 */
public class HiveConfig extends HdfsConfig {

    private static final String TABLE_NAME = "tableName";
    private static final String JOIN_KEY = "joinKey";
    private static final String USE_MEM = "useMem";
    private static final String JOIN_LOG_SWITCH = "joinLogSwitch";


    public static final ConfigOption<String> BSQL_HIVE_TABLE_NAME = ConfigOptions
            .key(TABLE_NAME)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql hive table name");


    public static final ConfigOption<String> BSQL_HIVE_JOIN_KEY = ConfigOptions
            .key(JOIN_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql hive join key");


    public static final ConfigOption<Boolean> BSQL_HIVE_CHECK_SUCCESS = ConfigOptions
            .key(CHECK_SUCCESS)
            .booleanType()
            .defaultValue(Boolean.FALSE)
            .withDescription("bsql hdfs check success");

    public static final ConfigOption<Boolean> BSQL_USE_MEM = ConfigOptions
            .key(USE_MEM)
            .booleanType()
            .defaultValue(Boolean.FALSE)
            .withDescription("bsql hive join use memory");


    public static final ConfigOption<Boolean> BSQL_JOIN_LOG = ConfigOptions
            .key(JOIN_LOG_SWITCH)
            .booleanType()
            .defaultValue(Boolean.FALSE)
            .withDescription("turn hive join exception and fail log");


}
