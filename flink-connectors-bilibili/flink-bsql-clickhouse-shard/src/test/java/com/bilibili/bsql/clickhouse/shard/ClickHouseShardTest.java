package com.bilibili.bsql.clickhouse.shard;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Objects;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/4 4:04 下午
 */
public class ClickHouseShardTest {

	@Test
	public void clickhouseShardTest() throws Exception {

		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is blink) indicates whether the " +
				"example uses flink planner or blink planner.");
			return;
		}
		tEnv.getConfig().getConfiguration().setBoolean("pipeline.object-reuse", true);


		String sql = "CREATE TABLE wxm_test (`tiny_int` TINYINT , `float_32` Float ,`float_64` Double,`log_date` String,`rn` int,`avid` bigint,`decimal_t` INT,`arrayString` as array[cast('test1' as String),cast('test2' as String ),cast('test3' as String)],`arrayInt` as array[1,23,121],`arrayFloat` as array[cast(1.11111 as float),cast(1.1111111 as float),cast(1.111111111 as float)],`data_t` as localtimestamp,`bi_t` TINYINT, `ts` as localtimestamp ,`start_time` as localtimestamp) WITH ('connector'='bsql-datagen','rows-per-second'='100')";
		tEnv.executeSql(sql);

		Table table2 = tEnv.sqlQuery("select tiny_int,float_32,float_64,log_date,rn,avid,arrayString,cast(1.1111 as Decimal(9,4)) as decimal_t,arrayInt,arrayFloat,CURRENT_DATE as data_t,ENCODE(cast(bi_t as String),'UTF-8') as bi_t,ts,start_time from wxm_test ");

		// clickhouse_shard 与 flink sql回归case共用，但是生产环境不支持BINARY
		String clickHouseSql = "CREATE TABLE sink (tiny_int tinyint,\n" +
			"  float_32 float,\n" +
			"  float_64 double,\n" +
			"  log_date String,\n" +
			"  rn int,\n" +
			"  avid bigint,\n" +
//			"  decimal_t Decimal(9,4),\n" +
//			"  arrayString Array<String>,\n" +
//			"  arrayInt Array<Int NOT NULL>,\n" +
//			"  arrayFloat Array<Float>,\n" +
//			"  data_t Date,\n" +
//			"  bi_t BINARY(4),\n" +
			"  `ts` Timestamp,start_time timestamp" +
			") WITH (" +
			"'connector'='bsql-clickhouse-shard'," +
			"'url'='clickhouse://10.221.50.169:8123'," +
			"'userName'='default'," +
			"'password'='123'," +
			"'tableName'='clickhouse_shard'," +
			"'databaseName'='default'," +
			"'batchMaxTimeout'='20000'," +
//			"'partitionStrategy'='hash'," +
			"'batchSize'='3'" +
			")";
		tEnv.executeSql(clickHouseSql);

//		tEnv.executeSql("INSERT INTO sink SELECT tiny_int,float_32,float_64,log_date,rn,avid,decimal_t,arrayString, arrayInt,arrayFloat,data_t,bi_t,`ts`,start_time FROM " + table2);
		tEnv.executeSql("INSERT INTO sink SELECT tiny_int,float_32,float_64,log_date,rn,avid,`ts`,start_time FROM " + table2);

		tEnv.toRetractStream(table2, Row.class).print("===== ");
		env.execute("");
	}

}
