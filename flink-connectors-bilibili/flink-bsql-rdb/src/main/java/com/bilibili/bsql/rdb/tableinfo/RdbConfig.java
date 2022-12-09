package com.bilibili.bsql.rdb.tableinfo;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RdbConfig.java
 * @description This is the description of RdbConfig.java
 * @createTime 2020-10-20 14:10:00
 */
public class RdbConfig {

	public static final String URL_KEY = "url";
	public static final String PASSWORD_KEY = "password";
	public static final String USER_NAME_KEY = "userName";
	public static final String CONNECTION_POOL_SIZE = "connectionPoolSize";
	public static final String DRIVER_NAME = "driverName";
	public static final String TABLE_NAME = "tableName";
	public static final String TPS = "tps";
	public static final String INSERT_TYPE = "insertType";
	public static final String REAL_TABLE_NAMES = "realTableNames";
	public static final String TARGET_DBS = "targetDBs";
	public static final String TARGET_TABLES = "targetTables";
	public static final String AUTO_COMMIT = "autoCommit";
	private static final String MAX_RETRIES = "maxRetries";


	public static final ConfigOption<String> BSQL_URL = ConfigOptions
		.key(URL_KEY)
		.stringType()
		.noDefaultValue()
		.withDescription("url");

	public static final ConfigOption<String> BSQL_PSWD = ConfigOptions
		.key(PASSWORD_KEY)
		.stringType()
		.defaultValue("")
		.withDescription("password");

	public static final ConfigOption<String> BSQL_UNAME = ConfigOptions
		.key(USER_NAME_KEY)
		.stringType()
		.defaultValue("")
		.withDescription("usename");

	public static final ConfigOption<Integer> BSQL_CONNECTION_POOL_SIZE = ConfigOptions
		.key(CONNECTION_POOL_SIZE)
		.intType()
		.defaultValue(10)
		.withDescription("connection pool size");

	public static final ConfigOption<String> BSQL_DRIVER_NAME = ConfigOptions
		.key(DRIVER_NAME)
		.stringType()
		.noDefaultValue()
		.withDescription("driver name");

	public static final ConfigOption<String> BSQL_TABLE_NAME = ConfigOptions
		.key(TABLE_NAME)
		.stringType()
		.noDefaultValue()
		.withDescription("table name");

	public static final ConfigOption<String> BSQL_TPS = ConfigOptions
		.key(TPS)
		.stringType()
		.noDefaultValue()
		.withDescription("tps");

	public static final ConfigOption<Integer> BSQL_INSERT_TYPE = ConfigOptions
		.key(INSERT_TYPE)
		.intType()
		.noDefaultValue()
		.withDescription("insert type");

	public static final ConfigOption<String> BSQL_REAL_TABLE_NAMES = ConfigOptions
		.key(REAL_TABLE_NAMES)
		.stringType()
		.noDefaultValue()
		.withDescription("real table names");

	public static final ConfigOption<String> BSQL_TARGET_DBS = ConfigOptions
		.key(TARGET_DBS)
		.stringType()
		.noDefaultValue()
		.withDescription("rdb target dbs");

	public static final ConfigOption<String> BSQL_TARGET_TABLES = ConfigOptions
		.key(TARGET_TABLES)
		.stringType()
		.noDefaultValue()
		.withDescription("target tables");

	public static final ConfigOption<String> BSQL_AUTO_COMMIT = ConfigOptions
		.key(AUTO_COMMIT)
		.stringType()
		.noDefaultValue()
		.withDescription("auto commit");
	public static final ConfigOption<Integer> BSQL_MYSQL_MAX_RETRIES = ConfigOptions
		.key(MAX_RETRIES)
		.intType()
		.defaultValue(3)
		.withDescription("bsql mysql maxRetries，default 3 times");
}
