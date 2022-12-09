package com.bilibili.bsql.format.delimit;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** BsqlDelimitConfig. */
public class BsqlDelimitConfig {
    private BsqlDelimitConfig() {}

    public static final ConfigOption<String> DELIMITER_KEY =
            ConfigOptions.key("delimiterKey")
                    .stringType()
                    .defaultValue("|")
                    .withDescription("the symbol used to split a line");

    public static final ConfigOption<Boolean> RAW_FORMAT =
            ConfigOptions.key("raw")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("use raw format");
    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("")
                    .withDescription("use row format");
    public static final ConfigOption<Boolean> NO_DEFAULT_VALUE =
            ConfigOptions.key("noDefaultValue")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("no default value in row data");

    public static final ConfigOption<Boolean> USE_LANCER_FORMAT =
            ConfigOptions.key("useLancerFormat")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("use lancer deserialization wrapper");
    public static final ConfigOption<Boolean> USE_LANCER_DEBUG =
            ConfigOptions.key("useLancerDebug")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("use lancer deserialization debug");

    public static final ConfigOption<String> SINK_SERIALIZER =
            ConfigOptions.key("serializer")
                    .stringType()
                    .defaultValue("")
                    .withDescription("custom define serializer");
}
