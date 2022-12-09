/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis;

import java.util.Objects;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;


/**
 *
 * @author zhouxiaogang
 * @version $Id: RedisSinkTest.java, v 0.1 2020-10-20 19:02
zhouxiaogang Exp $$
 */
public class RedisSinkTest {
	@Test
	public void sinkWorkability() throws Exception {
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
			"  `name` VARBINARY,\n" +
			"  `num` INT,\n" +
			"  primary key(num) NOT ENFORCED\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-redis',\n" +
			"  'redisType' = '3',\n" +
			"  'url' = '172.22.33.30:7899'\n" +

			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT ENCODE(name,'utf-8'),num FROM Source");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}

	@Test
	public void sinkRetract() throws Exception {
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
			"  `num` INT,\n" +
			"  `xtime` BIGINT\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'zzs-sink',\n" +
			"  'offsetReset' = 'latest',\n" +
			"  'bootstrapServers' = '172.22.33.94:9092,172.22.33.99:9092,172.22.33.97:9092',\n" +
			"  'parallelism' = '1',\n" +
			"  'bsql-delimit.delimiterKey' = '\\u0001'   -- declare a format for this system\n" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into MyUserTable " +
			"SELECT max(num),WEEK(xtime) FROM Source group by WEEK(xtime)");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}
}
