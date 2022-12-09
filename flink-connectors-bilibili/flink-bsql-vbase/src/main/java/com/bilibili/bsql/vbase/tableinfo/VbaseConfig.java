package com.bilibili.bsql.vbase.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author zhuzhengjun
 * @date 2020/11/3 2:20 下午
 */
public class VbaseConfig {

    private static final String HOST = "host";

    private static final String PORT = "port";

    private static final String PARENT = "parent";

    private static final String PREROWKEY = "preRowKey";

    private static final String TABLENAME = "tableName";

    private static final String ROWKEYNAME = "rowkeyName";

    private static final String TIMEOUTMS = "timeOutMs";


    public static final ConfigOption<String> BSQL_VBASE_HOST = ConfigOptions
            .key(HOST)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase host");

    public static final ConfigOption<String> BSQL_VBASE_PORT = ConfigOptions
            .key(PORT)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase port");

    public static final ConfigOption<String> BSQL_VBASE_PARENT = ConfigOptions
            .key(PARENT)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase parent");

    public static final ConfigOption<String> BSQL_VBASE_PREROWKEY = ConfigOptions
            .key(PREROWKEY)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase preRowKey");

    public static final ConfigOption<String> BSQL_VBASE_TABLENAME = ConfigOptions
            .key(TABLENAME)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase tableName");

    public static final ConfigOption<String> BSQL_VBASE_ROWKEYNAME = ConfigOptions
            .key(ROWKEYNAME)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase rowkeyName");

    public static final ConfigOption<String> BSQL_VBASE_TIMEOUTMS = ConfigOptions
            .key(TIMEOUTMS)
            .stringType()
            .noDefaultValue()
            .withDescription("bsql vbase timeOutMs");

}
