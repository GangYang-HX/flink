package com.bilibili.bsql.common.utils;

import org.apache.flink.configuration.ReadableConfig;

import org.apache.commons.lang3.StringUtils;

import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_PSWD;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_TABLE_NAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_UNAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_URL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** OptionUtils. */
public class OptionUtils {

    /** check rdb table config. */
    public static void checkTableConfig(ReadableConfig readableConfig) {
        String url = readableConfig.get(BSQL_URL);
        String userName = readableConfig.get(BSQL_UNAME);
        String password = readableConfig.get(BSQL_PSWD);
        String tableName = readableConfig.get(BSQL_TABLE_NAME);

        checkArgument(StringUtils.isNotEmpty(url), "表没有设置url属性");
        checkArgument(StringUtils.isNotEmpty(tableName), "表没有设置tableName属性");
        checkArgument(StringUtils.isNotEmpty(userName), "表没有设置userName属性");
        checkArgument(StringUtils.isNotEmpty(password), "表没有设置password属性");
    }
}
