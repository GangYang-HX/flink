/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.tableinfo;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlKafka010Config.java, v 0.1 2020-09-28 16:18
zhouxiaogang Exp $$
 */
public class BsqlKafka010Config {

	// Prefix for Kafka specific properties.
	public static final String PROPERTIES_PREFIX = "properties.";

	public BsqlKafka010Config() {}

	// --------------------------------------------------------------------------------------------
	// seperate the bsql options from open source, as the desc are different
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> BSQL_TOPIC = ConfigOptions
		.key("topic")
		.stringType()
		.noDefaultValue()
		.withDescription("Required topic name from which the table is read");

	public static final ConfigOption<String> BSQL_PROPS_BOOTSTRAP_SERVERS = ConfigOptions
		.key("bootstrapServers")
		.stringType()
		.noDefaultValue()
		.withDescription("Required Kafka server connection string");

	public static final ConfigOption<Boolean> BSQL_PROPS_KAFKA_RETRY = ConfigOptions
		.key("isFailureRetry")
		.booleanType()
		.defaultValue(true)
		.withDescription("kafka will retry after exception");
	public static final ConfigOption<String> BSQL_PROPS_KAFKA_SERIALIZER = ConfigOptions
		.key("serializer")
		.stringType()
		.defaultValue("")
		.withDescription("determine when use HashKeySerializationSchemaWrapper or HashKeyBytesSerializationSchemaWrapper");

	// --------------------------------------------------------------------------------------------
	// Scan specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> BSQL_GROUP_OFFSETS = ConfigOptions
		.key("offsetReset")
		.stringType()
		.defaultValue("latest")
		.withDescription("kafka的消费模式：支持latest,earliest,不填默认latest；支持13位时间戳，指定消费的起始时间");

	/**
	 * select default recover strategy when partition leader is -1
	 */
	public static final ConfigOption<String> BSQL_DEFAULT_OFFSET_RESET = ConfigOptions
		.key("defOffsetReset")
		.stringType()
		.noDefaultValue()
		.withDescription("默认为latest,可以自行指定为latest或者earliest");

	/**
	 * because we only use the delimit format
	 */
	public static final ConfigOption<String> BSQL_FORMAT = ConfigOptions
		.key("format")
		.stringType()
		.defaultValue("bsql-delimit")
		.withDescription("add a option to enable read format");


	public static final ConfigOption<Boolean> TOPIC_IS_PATTERN = ConfigOptions
		.key("topicIsPattern")
		.booleanType()
		.defaultValue(false)
		.withDescription("is topic pattern");

	public static final ConfigOption<String> PARTITION_STRATEGY = ConfigOptions
		.key("partitionStrategy")
		.stringType()
		.defaultValue("DISTRIBUTE")
		.withDescription("how records are partitioned");

	public static final ConfigOption<String> PARTITION_DISCOVER_RETRY = ConfigOptions
		.key("partitionDiscoverRetries")
		.stringType()
		.defaultValue("15")
		.withDescription("partition discover retry times.");
	public static final ConfigOption<String> CONSUMER_DETECT_REQUEST_TIMEOUT_MS = ConfigOptions
		.key("detectRequestTimeoutMs")
		.stringType()
		.defaultValue("30000")
		.withDescription("default 'request.timeout.ms' in kafka source consumer.");

    public static final ConfigOption<String> SINK_TRACE_ID =
        key("traceId")
            .stringType()
            .defaultValue("")
            .withDescription("traceId");

    public static final ConfigOption<String> SINK_TRACE_KIND=
        key("traceKind")
            .stringType()
            .defaultValue("")
            .withDescription("traceKind");

	public static final ConfigOption<String> SINK_CUSTOM_TRACE_CLASS=
        key("custom.trace.class")
            .stringType()
            .defaultValue("org.apache.flink.bili.external.trace.LancerTrace")
            .withDescription("custom.trace.class");

    public static final ConfigOption<Boolean> USE_LANCER_FORMAT =
        key("bsql-delimit.useLancerFormat")
            .booleanType()
            .defaultValue(false)
            .withDescription("use lancerDeserializationWrapper");
    public static final ConfigOption<String> LANCER_SINK_DEST =
        key("bsql-delimit.sinkDest")
            .stringType()
            .defaultValue("")
            .withDescription("lancer sink dest");

    public static final ConfigOption<String> BLACKLIST_ENABLE =
        key("blacklist.enable")
            .stringType()
            .defaultValue("false")
            .withDescription("enable kafka tp blacklist");

    public static final ConfigOption<String> BLACKLIST_ZK_HOST =
        key("blacklist.zk.host")
            .stringType()
            .defaultValue("jssz-failover-kafka-01.host.bilibili.co:2181,jssz-failover-kafka-02.host.bilibili.co:2181,jssz-failover-kafka-03.host.bilibili.co:2181,jssz-failover-kafka-04.host.bilibili.co:2181,jssz-failover-kafka-05.host.bilibili.co:2181")
            .withDescription("blacklist zk host");

    public static final ConfigOption<String> BLACKLIST_ZK_ROOT_PATH =
        key("blacklist.zk.root.path")
            .stringType()
            .defaultValue("/blacklist")
            .withDescription("blacklist zk host path");

    public static final ConfigOption<String> BLACKLIST_LAG_THRESHOLD =
        key("blacklist.lag.threshold")
            .stringType()
            .defaultValue("50000")
            .withDescription("kafka blacklist lag threshold");

    public static final ConfigOption<String> BLACKLIST_KICK_THRESHOLD =
        key("blacklist.kick.threshold")
            .stringType()
            .defaultValue("-1")
            .withDescription("kafka blacklist kick threshold");

    public static final ConfigOption<String> BLACKLIST_LAG_TIMES =
        key("blacklist.lag.times")
            .stringType()
            .defaultValue("10")
            .withDescription("kafka blacklist kick lag times");

    public static final ConfigOption<Boolean> IS_KAFKA_OVER_SIZE_RECORD_IGNORE =
       key("isKafkaRecordOversizeIgnore")
            .booleanType()
            .defaultValue(false)
            .withDescription("open the metric about kafka over size record");


	// --------------------------------------------------------------------------------------------
	// Validation
	// --------------------------------------------------------------------------------------------

	public static void validateBsqlTableOptions(ReadableConfig tableOptions) {
		Preconditions.checkNotNull(tableOptions.get(BSQL_TOPIC));
		Preconditions.checkNotNull(tableOptions.get(BSQL_PROPS_BOOTSTRAP_SERVERS));
//		Preconditions.checkArgument(tableOptions.get(BSQL_PARALLELISM) > 0);
	}


	public static Properties getKafkaProperties(Map<String, String> tableOptions) {
		final Properties kafkaProperties = new Properties();

		if (hasKafkaClientProperties(tableOptions)) {
			tableOptions.keySet().stream()
				.filter(key -> key.startsWith(PROPERTIES_PREFIX))
				.forEach(key -> {
					final String value = tableOptions.get(key);
					final String subKey = key.substring((PROPERTIES_PREFIX).length());
					kafkaProperties.put(subKey, value);
				});
		}
		return kafkaProperties;
	}

	/** Decides if the table options contains Kafka client properties that start with prefix 'properties'. */
	private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
		return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
	}

	// --------------------------------------------------------------------------------------------
	// Inner classes
	// --------------------------------------------------------------------------------------------

	/** Kafka startup options. **/
	public static class StartupOptions {
		public StartupMode startupMode;
		public Map<KafkaTopicPartition, Long> specificOffsets;
		public long startupTimestampMillis;
	}
}
