package com.bilibili.bsql.kafka.diversion.tableinfo;

import com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

public class BsqlKafka010DiversionConfig extends BsqlKafka010Config {

	private BsqlKafka010DiversionConfig() {
		super();
	}

	public static final ConfigOption<String> BSQL_TOPIC_UDF = ConfigOptions
		.key("topic.udf.class")
		.stringType()
		.defaultValue("")
		.withDescription("topic udf class name, if you want use it in topic properties");

	public static final ConfigOption<String> BSQL_BROKER_UDF = ConfigOptions
		.key("bootstrapServers.udf.class")
		.stringType()
		.defaultValue("")
		.withDescription("bootstrapServers udf class name, if you want use it in bootstrapServers properties");

	public static final ConfigOption<String> CACHE_TTL = ConfigOptions
		.key("udf.cache.minutes")
		.stringType()
		.defaultValue("60")
		.withDescription("udf cache ttl, set default value 1 hour.");

	public static final ConfigOption<Boolean> EXCLUDE_ARG_FIELD = ConfigOptions
		.key("exclude.udf.field")
		.booleanType()
		.defaultValue(false)
		.withDescription("exclude udf args field if set to true");

    public static final ConfigOption<String> BLACKLIST_ENABLE =
        key("blacklist.enable")
            .stringType()
            .defaultValue("false")
            .withDescription("enable kafka tp blacklist");

    public static final ConfigOption<String> BLACKLIST_ZK_HOST =
        key("blacklist.zk.host")
            .stringType()
            .defaultValue("")
            .withDescription("blacklist zk host");

    public static final ConfigOption<String> BLACKLIST_ZK_ROOT_PATH =
        key("blacklist.zk.root.path")
            .stringType()
            .defaultValue("/blacklist")
            .withDescription("blacklist zk host path");
}
