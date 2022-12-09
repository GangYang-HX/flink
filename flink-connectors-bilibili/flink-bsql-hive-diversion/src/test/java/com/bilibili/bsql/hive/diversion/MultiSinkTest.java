package com.bilibili.bsql.hive.diversion;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.util.Objects;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/4 6:20 下午
 */
public class MultiSinkTest {

	@Test
	public void multiSinkTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(60);
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

		tEnv.executeSql("CREATE TABLE Source (\n" +
			"  -- declare the schema of the table\n" +
			"  `name` STRING,\n" +
			"  `num` INT,\n" +
			"  `xtime` as proctime()\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `name` VARCHAR,\n" +
			"  `num` INT,\n" +
			"  primary key(num) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'timeField' = 'display_time',\n" +
//			"  'parallelism' = '60',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive-diversion',\n" +
			"  'partitionKey' = 'log_date,log_hour',\n" +
			"  'table.tag.udf' = 'GetTableTag(name,num)',\n" +
			"  'table.tag.udf.class' = 'com.bilibili.bsql.hive.diversion.GetTableTag',\n" +
			"  'partition.udf' = 'GetPartition(name)',\n" +
			"  'partition.udf.class' = 'com.bilibili.bsql.hive.diversion.GetPartition',\n" +
			"  'split.policy.udf' = 'CustomSplitPolicy()',\n" +
			"  'split.policy.udf.class' = 'com.bilibili.bsql.hive.diversion.CustomSplitPolicy',\n" +
			"  'table.meta.udf' = 'GetTableMeta()',\n" +
			"  'table.meta.udf.class' = 'com.bilibili.bsql.hive.diversion.GetTableMeta'\n" +
//			"  'tableName' = 'ai.feature_merge_wdlks_multitask_pv'\n" +
			")");

		StatementSet stateSet = tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT name,num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}
}

