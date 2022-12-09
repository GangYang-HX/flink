
package com.bilibili.bsql.kafka.diversion.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** BsqlKafkaDiversionConfig. */
public class BsqlKafkaDiversionConfig {

    private BsqlKafkaDiversionConfig() {}

    public static final ConfigOption<String> BSQL_TOPIC_UDF =
            ConfigOptions.key("topic.udf.class")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "topic udf class name, if you want use it in topic properties");

    public static final ConfigOption<String> BSQL_BROKER_UDF =
            ConfigOptions.key("bootstrapServers.udf.class")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "bootstrapServers udf class name, if you want use it in bootstrapServers properties");

    public static final ConfigOption<String> CACHE_TTL =
            ConfigOptions.key("udf.cache.minutes")
                    .stringType()
                    .defaultValue("60")
                    .withDescription("udf cache ttl, set default value 1 hour.");

    public static final ConfigOption<Boolean> EXCLUDE_ARG_FIELD =
            ConfigOptions.key("exclude.udf.field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("exclude udf args field if set to true");

    public static final ConfigOption<String> SINK_TRACE_ID =
            ConfigOptions.key("traceId").stringType().defaultValue("").withDescription("traceId");

    public static final ConfigOption<String> SINK_TRACE_KIND =
            ConfigOptions.key("traceKind")
                    .stringType()
                    .defaultValue("")
                    .withDescription("traceKind");

    public static final ConfigOption<String> SINK_CUSTOM_TRACE_CLASS =
            ConfigOptions.key("custom.trace.class")
                    .stringType()
                    .defaultValue("com.bilibili.bsql.trace.LancerTrace")
                    .withDescription("custom.trace.class");
}
