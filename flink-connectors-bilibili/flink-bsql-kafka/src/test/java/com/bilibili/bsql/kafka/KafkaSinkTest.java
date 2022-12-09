/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.Objects;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;


/**
 *
 * @author zhouxiaogang
 * @version $Id: KafkaSinkTest.java, v 0.1 2020-10-20 19:02
zhouxiaogang Exp $$
 */
public class KafkaSinkTest {
	@Test
	public void sinkWorkability() throws Exception {
		Duration duration = Duration.create("30 s");
		System.err.println(duration);


		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		Configuration configuration = new Configuration();
		configuration.setString(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, "30 sec");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
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

		tEnv.executeSql("CREATE TABLE source_test (\n" +
			"  id BIGINT,\n" +
			"  topic_name string metadata from 'topic', \n" +
			" `partition_id` INT METADATA FROM 'partition',\n" +
			"  mock_time TIMESTAMP(3),\n" +
			"  `offset` BIGINT METADATA,\n" +
			"  `timestamp` TIMESTAMP(3) METADATA,\n" +
			" `timestamp-type` string METADATA,\n" +
			"  computed_column AS PROCTIME(),\n" +
			"  `headers` MAP<STRING, BYTES> METADATA,\n" +
			"  WATERMARK FOR mock_time AS mock_time - INTERVAL '5' SECOND\n" +
			") WITH(\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'meimeizitest1',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'bsql-delimit.delimiterKey' = ','" +
			")");


		tEnv.executeSql("CREATE TABLE sink_test (\n" +
			"  id BIGINT,\n" +
			"  topic_name string ,\n" +
			" `partition_id` INT ,\n" +
			"  mock_time TIMESTAMP(3),\n" +
			"  `offset` BIGINT ,\n" +
			"  `timestamp` TIMESTAMP(3),\n" +
			" `timestamp-type` string,\n" +
			"  computed_column TIMESTAMP(3),\n" +
			"  `headers` MAP<STRING, BYTES>\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'meimeizitest2',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'bsql-delimit.delimiterKey' = ','" +
			")");


		/*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
		env.execute();*/

		StatementSet stateSet = tEnv.createStatementSet();
		stateSet.addInsertSql("INSERT INTO sink_test " +
			"SELECT " +
			"a.id," +
			"a.topic_name," +
			"a.partition_id," +
			"a.mock_time," +
			"a.`offset`," +
			"a.`timestamp`," +
			"a.`timestamp-type`," +
			"a.computed_column," +
			"a.`headers`" +
			" FROM source_test as a");
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

//		env.setStreamTimeCharacteristic(EventTime);
		tEnv.executeSql("create function to_timestamp as 'com.bilibili.bsql.kafka.ToTimestamp' language java");
		tEnv.executeSql("create function now_ts as 'com.bilibili.bsql.kafka.NowTs' language java");
		tEnv.executeSql("create function now_unix as 'com.bilibili.bsql.kafka.NowUnixtime' language java");

		tEnv.executeSql("CREATE TABLE Source (\n" +
			"  -- declare the schema of the table\n" +
			"  `name` STRING,\n" +
			"  `num` INT,\n" +
//							"  `xtime` as proctime()\n"+
				"  `xtime` as now_ts(),\n"+
				"  WATERMARK FOR xtime as xtime - INTERVAL '10.000' SECOND\n"+
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `num` INT,\n" +
			"  `xtime` TIMESTAMP(3)\n" +
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
			"SELECT max(num),TUMBLE_START(xtime, INTERVAL '20' SECOND) " +
				"FROM Source group by TUMBLE(xtime, INTERVAL '20' SECOND)");

		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void lancerTest() throws Exception {
		Duration duration = Duration.create("30 s");
		System.err.println(duration);


		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		Configuration configuration = new Configuration();
		configuration.setString(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, "30 sec");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
		env.setParallelism(1);

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

//		tEnv.executeSql("create function lancerConcat as 'com.bilibili.bsql.kafka.LancerConcat' language java");
		tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
			"  `body` bytes,\n" +
			"  `timestamp` TIMESTAMP(3) METADATA,\n" +
			" `timestamp-type` string METADATA,\n" +
			"  `headers` MAP<varchar,bytes> METADATA,\n" +
			"  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
			"WATERMARK  FOR ts as ts- INTERVAL '0.1' SECOND \n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'zzj_test',\n" +
			"  'offsetReset' = 'latest',\n" +
			//"  'format' = 'bytes',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
//			"  'bsql-delimit.delimiterKey' = '|',   -- declare a format for this system\n" +
			"  'bsql-delimit.format' = 'bytes',\n" +
			"  'bsql-delimit.useLancerFormat' = 'true',\n" +
			"  'bsql-delimit.useLancerDebug' = 'true',\n" +
			"  'bsql-delimit.traceId' = '714210',\n" +
			"  'bsql-delimit.sinkDest' = 'KAFKA' \n" +
			")");


		tEnv.executeSql("CREATE TABLE sink_test (\n" +
			"  body bytes,\n" +
			"  `timestamp` TIMESTAMP(3) METADATA,\n" +
			"  `headers` MAP<STRING, BYTES> METADATA\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'meimeizitest2',\n" +
			"  'serializer' = 'bytes',\n" +
			"  'partitionStrategy' = 'BUCKET',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'bsql-delimit.delimiterKey' = ','" +
			")");


		/*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
		env.execute();*/

		StatementSet stateSet = tEnv.createStatementSet();
		stateSet.addInsertSql("INSERT INTO sink_test " +
			"SELECT " +
			"a.body," +
			"a.`timestamp`," +
			"a.`headers`" +
			" FROM my_kafka_test  a\n");
		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}


	@Test
	public void autoFillingBootstrapServer() throws Exception {
		Duration duration = Duration.create("30 s");
		System.err.println(duration);


		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		Configuration configuration = new Configuration();
		configuration.setString(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, "30 sec");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
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

		tEnv.executeSql("CREATE TABLE source_test (\n" +
				"  id BIGINT,\n" +
				"  topic_name string metadata from 'topic', \n" +
				" `partition_id` INT METADATA FROM 'partition',\n" +
				"  mock_time TIMESTAMP(3),\n" +
				"  `offset` BIGINT METADATA,\n" +
				"  `timestamp` TIMESTAMP(3) METADATA,\n" +
				" `timestamp-type` string METADATA,\n" +
				"  computed_column AS PROCTIME(),\n" +
				"  `headers` MAP<STRING, BYTES> METADATA,\n" +
				"  WATERMARK FOR mock_time AS mock_time - INTERVAL '5' SECOND\n" +
				") WITH(\n" +
				"  'connector' = 'bsql-kafka10',\n" +
				"  'topic' = 'meimeizitest1',\n" +
//				"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
				"  'bsql-delimit.delimiterKey' = ','" +
				")");


		tEnv.executeSql("CREATE TABLE sink_test (\n" +
				"  id BIGINT,\n" +
				"  topic_name string ,\n" +
				" `partition_id` INT ,\n" +
				"  mock_time TIMESTAMP(3),\n" +
				"  `offset` BIGINT ,\n" +
				"  `timestamp` TIMESTAMP(3),\n" +
				" `timestamp-type` string,\n" +
				"  computed_column TIMESTAMP(3),\n" +
				"  `headers` MAP<STRING, BYTES>\n" +
				") WITH (\n" +
				"  'connector' = 'bsql-kafka10',\n" +
				"  'topic' = 'meimeizitest2',\n" +
//				"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
				"  'bsql-delimit.delimiterKey' = ','" +
				")");


		/*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
		env.execute();*/

		StatementSet stateSet = tEnv.createStatementSet();
		stateSet.addInsertSql("INSERT INTO sink_test " +
				"SELECT " +
				"a.id," +
				"a.topic_name," +
				"a.partition_id," +
				"a.mock_time," +
				"a.`offset`," +
				"a.`timestamp`," +
				"a.`timestamp-type`," +
				"a.computed_column," +
				"a.`headers`" +
				" FROM source_test as a");
		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
	}
}
