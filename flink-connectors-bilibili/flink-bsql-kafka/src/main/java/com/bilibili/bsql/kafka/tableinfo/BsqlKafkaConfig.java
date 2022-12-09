package com.bilibili.bsql.kafka.tableinfo;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Related configuration of bsql and its default value. */
public class BsqlKafkaConfig {
    public static final ConfigOption<Boolean> BSQL_PROPS_KAFKA_RETRY =
            key("isFailureRetry")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("kafka will retry after exception");

    public static final ConfigOption<Integer> BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT =
            key("pendingRetryRecordsLimit")
                    .intType()
                    .defaultValue(5_000_000)
                    .withDescription("pending retry records limit");

    public static final ConfigOption<String> PARTITION_STRATEGY =
            key("partitionStrategy")
                    .stringType()
                    .defaultValue("DISTRIBUTE")
                    .withDescription("how records are partitioned");

    public static final ConfigOption<String> SINK_DEST =
            key("sinkDest").stringType().defaultValue("").withDescription("sink destination");

    public static final ConfigOption<String> TRACE_ID =
            key("traceId").stringType().defaultValue("").withDescription("traceId");

    public static final ConfigOption<String> TRACE_KIND =
            key("traceKind").stringType().defaultValue("").withDescription("traceKind");

    public static final ConfigOption<String> CUSTOM_TRACE_CLASS =
            key("custom.trace.class")
                    .stringType()
                    .defaultValue("com.bilibili.bsql.trace.LancerTrace")
                    .withDescription("custom.trace.class");

    public static final ConfigOption<Boolean> USE_LANCER_DESERIALIZATION_SCHEMA =
            key("lancer.deserialization.schema")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("identify whether use lancer format to parse event");

    public static final ConfigOption<String> BSQL_OFFSET_RESET =
            key("offsetReset")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription(
                            "kafka consumer policy:latest,earliest,latest default;A 13-bit timestamp is supported, specifying the start time of consumption");

    public static final ConfigOption<String> BSQL_DEFAULT_OFFSET_RESET =
            key("defOffsetReset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("specify latest or earliest, with no default value");
}
