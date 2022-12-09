package com.bilibili.bsql.lanceragent.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className LancerAgentConfig.java
 * @description This is the description of LancerAgentConfig.java
 * @createTime 2020-11-10 18:23:00
 */
public class LancerAgentConfig {

    public static final String LOG_ID = "logId";
    private static final String DELIMITER = "bsql-delimit.delimiterKey";

    public static final ConfigOption<String> BSQL_LANCER_AGENT_DELIMITER = ConfigOptions
            .key(DELIMITER)
            .stringType()
            .defaultValue("|")
            .withDescription("lancer agent delimiter");

    public static final ConfigOption<String> BSQL_LANCER_AGENT_LOG_ID = ConfigOptions
            .key(LOG_ID)
            .stringType()
            .noDefaultValue()
            .withDescription("lancer agent delimiter");
}
