package com.bilibili.bsql.kafka.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Related configuration of bsql and its default value. */
public class BsqlKafkaConfig {
    public static final ConfigOption<Boolean> BSQL_PROPS_KAFKA_RETRY =
            ConfigOptions.key("isFailureRetry")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("kafka will retry after exception");

    public static final ConfigOption<Integer> BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT =
            ConfigOptions.key("pendingRetryRecordsLimit")
                    .intType()
                    .defaultValue(5_000_000)
                    .withDescription("pending retry records limit");

    public static final ConfigOption<String> PARTITION_STRATEGY =
            ConfigOptions.key("partitionStrategy")
                    .stringType()
                    .defaultValue("DISTRIBUTE")
                    .withDescription("how records are partitioned");

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
