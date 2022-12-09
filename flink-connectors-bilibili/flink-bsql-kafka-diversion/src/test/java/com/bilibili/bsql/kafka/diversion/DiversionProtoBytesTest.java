package com.bilibili.bsql.kafka.diversion;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

import static com.bilibili.bsql.common.keys.TableInfoKeys.SABER_JOB_ID;
import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;

public class DiversionProtoBytesTest {

	@Test
	public void diversionProtoBytesSqlTest () throws InterruptedException {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		Configuration flinkConfig = new Configuration();
		flinkConfig.set(SABER_JOB_ID, "for test");

		TableConfig customTableConfig = new TableConfig();
		customTableConfig.addConfiguration(flinkConfig);
		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) { // use blink planner in streaming mode
			EnvironmentSettings settings =
				EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
			tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
		} else if (Objects.equals(planner, "flink")) { // use flink planner in streaming mode
			EnvironmentSettings settings =
				EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
			tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
		} else {
			System.err.println(
				"The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', "
					+ "where planner (it is either flink or blink, and the default is blink) indicates whether the "
					+ "example uses flink planner or blink planner.");
			return;
		}

		env.setStreamTimeCharacteristic(EventTime);
		env.getConfig().setParallelism(1);

		Table table =
			tEnv.fromValues(
					Arrays.asList(
						Row.of("1","1","a", 1, 1636452123634L, "2021-11-09 19:26:12.000"),
						Row.of("1","2","b", 2, 1636445572634L, "2021-11-09 19:30:12.000"),
						Row.of("2","1","c", 3, 1631326562634L, "2021-11-09 19:31:12.000"),
						Row.of("2","2","d", 4, 1636457172634L, "2021-11-09 19:32:12.000")))
				.as("broker_id","topic_id","name", "num", "xtime", "timestamp");
		tEnv.createTemporaryView("my_kafka_source", table);

		tEnv.executeSql(
			"CREATE TABLE pb_diversion_sink_test (\n" +
				"  `broker_arg` STRING,\n" +
				"  `topic_arg` STRING,\n" +
				"  `name` STRING,\n" +
				"  `num` INT,\n" +
				"  `xtime` bigint,\n" +
				"  `timestamp` timestamp(3) metadata ,\n" +
				"  primary key(name) not enforced\n" +
				") WITH (\n" +
				"  'offsetReset' = 'earliest',\n" +
				"  'connector' = 'bsql-kafka10-diversion',\n" +
				"  'bootstrapServers' = 'BrokerUdf(broker_arg)',\n" +
				"  'bootstrapServers.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf',\n" +
				"  'topic' = 'TopicUdf(broker_arg, topic_arg)',\n" +
				"  'topic.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.TopicUdf',\n" +
				"  'udf.cache.min' = '1',\n" +
				"  'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.KafkaDiversionSinkTestOuterClass$KafkaDiversionSinkTest',\n" +
				"  'serializer' = 'proto_bytes',\n" +
				"  'format' = 'protobuf', \n" +
				"  'bsql-delimit.delimiterKey' = '|'" +
				")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("INSERT INTO pb_diversion_sink_test SELECT " +
			"CONCAT('cluster', cast(broker_id as VARCHAR(1))), " +
			"CONCAT('topic', cast(topic_id as VARCHAR(1))), " +
			"name,num,xtime,TO_TIMESTAMP(`timestamp`) as `timestamp` " +
			"from my_kafka_source" +
			"");

		stateSet.execute("pb diversion sink job");
		Thread.sleep(10000);
	}

	@Test
	public void resultQuery() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		Configuration flinkConfig = new Configuration();
		flinkConfig.set(SABER_JOB_ID, "for test");

		TableConfig customTableConfig = new TableConfig();
		customTableConfig.addConfiguration(flinkConfig);
		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) { // use blink planner in streaming mode
			EnvironmentSettings settings =
				EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
			tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
		} else if (Objects.equals(planner, "flink")) { // use flink planner in streaming mode
			EnvironmentSettings settings =
				EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
			tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
		} else {
			System.err.println(
				"The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', "
					+ "where planner (it is either flink or blink, and the default is blink) indicates whether the "
					+ "example uses flink planner or blink planner.");
			return;
		}

		env.setStreamTimeCharacteristic(EventTime);
		env.getConfig().setParallelism(1);
		tEnv.executeSql(
			"CREATE TABLE pb_diversion_result (\n" +
				"  `broker_arg` STRING,\n" +
				"  `topic_arg` STRING,\n" +
				"  `name` STRING,\n" +
				"  `num` INT,\n" +
				"  `xtime` bigint,\n" +
				"  `timestamp` timestamp(3) metadata ,\n" +
				"  primary key(name) not enforced\n" +
				") WITH (\n" +
				"  'offsetReset' = 'latest',\n" +
				"  'connector' = 'bsql-kafka10',\n" +
				"  'bootstrapServers' = '10.221.51.174:9092',\n" +
				"  'bootstrapServers.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf',\n" +
				"  'topic' = 'cluster1_1,cluster1_2,cluster2_1,cluster2_2',\n" +
				"  'udf.cache.min' = '1',\n" +
				"  'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.KafkaDiversionSinkTestOuterClass$KafkaDiversionSinkTest',\n" +
				"  'serializer' = 'proto_bytes',\n" +
				"  'format' = 'protobuf' \n" +
				")");

		String sql = "SELECT * FROM pb_diversion_result";
		Table result = tEnv.sqlQuery(sql);
		DataStream<Row> output = tEnv.toAppendStream(result, Row.class);
		output.print();

		env.execute();
	}
}
