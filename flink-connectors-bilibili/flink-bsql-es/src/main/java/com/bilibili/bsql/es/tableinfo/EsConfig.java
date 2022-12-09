package com.bilibili.bsql.es.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className EsConfig.java
 * @description This is the description of EsConfig.java
 * @createTime 2020-10-28 11:15:00
 */
public class EsConfig {

	private static final String ADDRESS = "address";
	private static final String INDEX_NAME = "indexName";
	private static final String TYPE_NAME = "typeName";

	public static final ConfigOption<String> BSQL_ES_ADDRESS = ConfigOptions
			.key(ADDRESS)
			.stringType()
			.noDefaultValue()
			.withDescription("es address");

	public static final ConfigOption<String> BSQL_ES_INDEX_NAME = ConfigOptions
			.key(INDEX_NAME)
			.stringType()
			.noDefaultValue()
			.withDescription("es indexName");

	public static final ConfigOption<String> BSQL_ES_TYPE_NAME = ConfigOptions
			.key(TYPE_NAME)
			.stringType()
			.defaultValue("logs")
			.withDescription("es typeName");
}
