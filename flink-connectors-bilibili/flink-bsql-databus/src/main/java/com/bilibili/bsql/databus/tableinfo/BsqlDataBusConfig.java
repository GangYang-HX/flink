package com.bilibili.bsql.databus.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;


/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlDataBusConfig.java
 * @description This is the description of BsqlDataBusConfig.java
 * @createTime 2020-10-22 17:45:00
 */
public class BsqlDataBusConfig {

	public static final String TOPIC_KEY = "topic";
	public static final String GROUPID_KEY = "groupId";
	public static final String APPKEY_KEY = "appKey";
	public static final String APPSECRET_KEY = "appSecret";
	public static final String ADDRESS_KEY = "address";
	public static final String DELIMITER_KEY = "bsql-delimit.delimiterKey";
	public static final String FORMAT = "format";
	public static final String PARTITION = "partition";

	public BsqlDataBusConfig() {
	}

	public static final ConfigOption<String> BSQL_DATABUS_TOPIC_KEY = ConfigOptions
			.key(TOPIC_KEY)
			.stringType()
			.noDefaultValue()
			.withDescription("bsql databus topic key");

	public static final ConfigOption<String> BSQL_DATABUS_GROUPID_KEY = ConfigOptions
		.key(GROUPID_KEY)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql databus groupId key");

	public static final ConfigOption<String> BSQL_DATABUS_APPKEY_KEY = ConfigOptions
		.key(APPKEY_KEY)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql databus appkey key");

	public static final ConfigOption<String> BSQL_DATABUS_APPSECRET_KEY = ConfigOptions
		.key(APPSECRET_KEY)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql databus app secret key");

	public static final ConfigOption<String> BSQL_DATABUS_ADDRESS_KEY = ConfigOptions
		.key(ADDRESS_KEY)
		.stringType()
		.noDefaultValue()
		.withDescription("bsql databus address key");

	public static final ConfigOption<String> BSQL_DATABUS_DELIMITER_KEY = ConfigOptions
		.key(DELIMITER_KEY)
		.stringType()
		.defaultValue("|")
		.withDescription("bsql databus delimiter key");

	public static final ConfigOption<String> BSQL_FORMAT = ConfigOptions
			.key(FORMAT)
			.stringType()
			.defaultValue("bsql-delimit")
		.withDescription("add a option to enable read format");

	public static final ConfigOption<Integer> BSQL_DATABUS_PARTITION_NUM = ConfigOptions
		.key(PARTITION)
		.intType()
		.noDefaultValue()
		.withDescription("databus topic partition number");

}
