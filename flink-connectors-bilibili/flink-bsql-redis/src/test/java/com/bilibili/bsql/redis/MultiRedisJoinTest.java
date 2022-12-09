/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */

package com.bilibili.bsql.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.operators.join.latency.ProcTimeGlobalJoinWithTTL;

import org.junit.Test;

/**
 * @author zhouhuidong
 * @version $Id: ListTypeOrcTest.java, v 0.1 2021-03-03 9:36 下午 zhouhuidong Exp $$
 */
public class MultiRedisJoinTest {

	@Test
	public void mutiRedisJoinTest() throws Exception {

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;

		TableConfig config = new TableConfig();
		Configuration configuration = new Configuration();
		//		172.22.33.30:7899;172.23.47.13:7115
		configuration.setString("latency.redis","172.22.33.30:7899;172.23.47.13:7115");
		configuration.setString("latency.class", ProcTimeGlobalJoinWithTTL.LABEL);
		configuration.setString("latency.redis.type","5");
		configuration.setString("latency.redis.bucket.size","1");
		configuration.setString("latency.save.key","true");
		configuration.setString("latency.merge.function","merge");
		config.addConfiguration(configuration);

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();
		tEnv = StreamTableEnvironment.create(env, config);


		tEnv.executeSql("CREATE TABLE Source1 (\n" +
			"  -- declare the schema of the table\n" +
			"  `key1` INT,\n" +
			"  `value1` INT,\n" +
			"  `ts` as proctime()\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '100',\n" +
			"  'fields.key1.min'='1',\n" +
			"  'fields.key1.max'='100'\n" +
			")\n" +
			"");

		tEnv.executeSql("CREATE TABLE Source2 (\n" +
			"  -- declare the schema of the table\n" +
			"  `key2` INT,\n" +
			"  `value2` INT,\n" +
			"  `ts` as proctime()\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '100',\n" +
			"  'fields.key2.min'='1',\n" +
			"  'fields.key2.max'='100'\n" +
			")");

		tEnv.executeSql("CREATE TABLE sinkkkk (\n" +
			"  -- declare the schema of the table\n" +
			"  `akey` INT,\n" +
			"  `avalue` INT,  \n" +
			"  `bkey` INT,\n" +
			"  `bvalue` INT,\n" +
			"  primary key(akey) NOT ENFORCED\n" +
			") WITH ('connector' = 'bsql-log')");
		tEnv.executeSql("CREATE VIEW joined_click AS\n" +
			"SELECT\n" +
			"  a.`key1`,\n" +
			"  a.`value1`,\n" +
			"  b.`key2`,\n" +
			"  b.`value2`\n" +
			"FROM\n" +
			"  Source1 a\n" +
			"LEFT JOIN Source2 b GLOBAL ON a.key1 = b.key2\n" +
			"  AND a.ts BETWEEN b.ts - INTERVAL '10' SECOND  AND b.ts\n" +
			"  AND b.ts BETWEEN a.ts - INTERVAL '10' SECOND AND a.ts");


		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("insert into sinkkkk " +
			"select * from joined_click");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void mutiRedisJoinTest2() throws Exception {

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(8);
		StreamTableEnvironment tEnv;

		TableConfig config = new TableConfig();
		Configuration configuration = new Configuration();
		//		172.22.33.30:7899;172.23.47.13:7115
//		configuration.setString("latency.redis","172.22.33.30:7899;172.23.47.13:7115");
//		configuration.setString("latency.class", ProcTimeGlobalJoinWithTTL.LABEL);
//		configuration.setString("latency.redis.type","5");
//		configuration.setString("latency.redis.bucket.size","1");
//		configuration.setString("latency.save.key","true");
		configuration.setString("join.latency.merge.function", "merge");
		configuration.setString("join.latency.merge.delimiter", "\u0001");
		configuration.setString("table.exec.mini-batch.enabled", "false");
		config.addConfiguration(configuration);

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
		tEnv = StreamTableEnvironment.create(env, config);


		tEnv.executeSql("CREATE TABLE Source1 (\n" +
				"  -- declare the schema of the table\n" +
				"  `key1` INT,\n" +
				"  `value1` VARCHAR,\n" +
				"  `ts` as proctime()\n" +
				") WITH (\n" +
				"  'connector' = 'bsql-datagen',\n" +
				"  'rows-per-second' = '200',\n" +
				"  'fields.key1.min'='1',\n" +
				"  'fields.key1.max'='100'\n" +
				")\n" +
				"");

		tEnv.executeSql("CREATE TABLE Source2 (\n" +
				"  -- declare the schema of the table\n" +
				"  `key2` INT,\n" +
				"  `value2` VARCHAR,\n" +
				"  `ts` as proctime()\n" +
				") WITH (\n" +
				"  'connector' = 'bsql-datagen',\n" +
				"  'rows-per-second' = '200',\n" +
				"  'fields.key2.min'='1',\n" +
				"  'fields.key2.max'='100'\n" +
				")");

		tEnv.executeSql("CREATE TABLE sinkkkk (\n" +
				"  -- declare the schema of the table\n" +
				"  `akey` INT,\n" +
				"  `avalue` VARCHAR,  \n" +
				"  `bkey` INT,\n" +
				"  `bvalue` VARCHAR,\n" +
				"  primary key(akey) NOT ENFORCED\n" +
				") WITH ('connector' = 'bsql-log')");
		tEnv.executeSql("CREATE VIEW joined_click AS\n" +
				"SELECT\n" +
				"  a.`key1`,\n" +
				"  a.`value1`,\n" +
				"  b.`key2`,\n" +
				"  b.`value2`\n" +
				"FROM\n" +
				"  Source1 a\n" +
				"LEFT JOIN Source2 b LATENCY ON a.key1 = b.key2\n" +
				"  and a.ts > b.ts - INTERVAL '1' MINUTE\n" +
				"  and a.ts < b.ts + INTERVAL '1' MINUTE");
		// "  AND a.ts BETWEEN b.ts - INTERVAL '10' SECOND  AND b.ts\n" +
		// "  AND b.ts BETWEEN a.ts - INTERVAL '10' SECOND AND a.ts");

		StatementSet stateSet = tEnv.createStatementSet();
		stateSet.addInsertSql("insert into sinkkkk " +
				"select * from joined_click");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


}
