/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package com.bilibili.bsql.hive;

import java.util.List;
import java.util.Objects;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.bili.writer.util.PartTimeUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 *
 * @author zhouhuidong
 * @version $Id: ListTypeOrcTest.java, v 0.1 2021-03-03 9:36 下午 zhouhuidong Exp $$
 */
public class ListTypeOrcTest {
	@Test
	public void normalTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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
			"  `xtime` as proctime()\n"+
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
			"  'parallelism' = '5',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive',\n" +
			"  'partitionKey' = 'log_date,log_hour',\n" +
			"  'partition.udf' = 'PartitionUdf(name)',\n" +
			"  'partition.udf.class' = 'com.bilibili.bsql.hive.PartitionUdf',\n" +
			"  'tableName' = 'ai.feature_merge_wdlks_multitask_pv'\n" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT name,num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void arrayTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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
			"  `xtime` as proctime()\n"+
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `nums` ARRAY<INT>,\n" +
			"  `num` INT,\n" +
			"  primary key(num) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'timeField' = 'display_time',\n" +
			"  'parallelism' = '5',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive',\n" +
			"  'tableName' = 'tmp_bdp.orc_test3'\n" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT Array[num,num],num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void nullTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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
			"  `xtime` as proctime()\n"+
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '100'\n" +
			")");

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `nums` VARCHAR,\n" +
			"  `num` INT,\n" +
			"  primary key(num) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'timeField' = 'display_time',\n" +
			"  'parallelism' = '5',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive',\n" +
			"  'tableName' = 'tmp_bdp.orc_test3'\n" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT name,num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void structTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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
			"  `xtime` as proctime()\n"+
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `nums` ROW<str VARCHAR,lg INT>,\n" +
			"  `num` INT,\n" +
			"  primary key(num) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'timeField' = 'display_time',\n" +
			"  'parallelism' = '5',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive',\n" +
			"  'tableName' = 'tmp_bdp.orc_test3'\n" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT Row('ss',123) as obj ,num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void mapTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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
			"  `xtime` as proctime()\n"+
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `nums` MAP<VARCHAR,INT>,\n" +
			"  `num` INT,\n" +
			"  primary key(num) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'timeField' = 'display_time',\n" +
			"  'parallelism' = '5',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive',\n" +
			"  'tableName' = 'tmp_bdp.orc_test3'\n" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT Map['k1',111,'k2',222] as `map` ,num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}
}
