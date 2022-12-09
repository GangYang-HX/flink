package com.bilibili.bsql.clickhouse;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Objects;


/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlClickHouseTest.java
 * @description This is the description of BsqlClickHouseTest.java
 * @createTime 2020-10-21 16:31:00
 */
public class BsqlClickHouseTest {

	@Test
	public void clickHouseTest() throws Exception {
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


		/*String sql = "CREATE TABLE wxm_test (`userss` BIGINT, `product` BIGINT,amount as array['1','2','3']) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
		tEnv.executeSql(sql);*/

		tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
				"  `name` VARCHAR,\n" +
				"  `num` int,\n" +
				"  `xtime` TIMESTAMP,\n" +
				"WATERMARK  FOR xtime as xtime- INTERVAL '1' SECOND \n"+
				") WITH (\n" +
				"  'connector' = 'bsql-kafka10',\n" +
				"  'topic' = 'flink_sql_kafka_sink',\n" +
				"  'offsetReset' = 'latest',\n" +
				"  'bootstrapServers' = '127.0.0.1:9092',\n" +
				"  'bsql-delimit.delimiterKey' = '|'   -- declare a format for this system\n" +
				")");

		Table table2 = tEnv.sqlQuery("select * from my_kafka_test");

		String clickHouseSql = "CREATE TABLE wxm_ck_test (info_index BIGINT,courier_id BIGINT,city_id VARCHAR" +
				") WITH (" +
				"'connector'='bsql-clickhouse'," +
				"'url'='jdbc:clickhouse://10.221.50.169:8123/warship'," +
				"'userName'='default'," +
				"'password'='123'," +
				"'tableName'='wxm_ck_test'" +
				")";
		tEnv.executeSql(clickHouseSql);

		tEnv.executeSql("insert into wxm_ck_test select count(num) as info_index,sum(num) as courier_id,name from " + table2 + " group by name");
		tEnv.toRetractStream(table2, Row.class).print("===== ");
		env.execute("");
	}
}
