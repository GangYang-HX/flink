package com.bilibili.bsql.kafka.diversion;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

public class DiversionTest {
	@Test
	public void testDiversionProducer() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);


		tEnv.executeSql("CREATE TABLE source_test (\n" +
			"  broker_id BIGINT,\n" +
			"  topic_id BIGINT\n" +
			") WITH(\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1',\n" +
			"  'fields.broker_id.min' = '1',\n" +
			"  'fields.broker_id.max' = '2',\n" +
			"  'fields.topic_id.min' = '1',\n" +
			"  'fields.topic_id.max' = '2'\n" +
			")");

		tEnv.executeSql("CREATE TABLE sink_test (\n" +
			"  `timestampcc` STRING,\n" +
			"  `broker_arg` STRING,\n" +
			"  `topic_arg` STRING\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10-diversion',\n" +
			"  'bootstrapServers' = 'BrokerUdf(broker_arg)',\n" +
			"  'bootstrapServers.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf',\n" +
			"  'topic' = 'TopicUdf(broker_arg, topic_arg)',\n" +
			"  'topic.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.TopicUdf',\n" +
			//"  'exclude.udf.field' = 'false',\n" +
			"  'udf.cache.min' = '1',\n" +
			"  'bsql-delimit.delimiterKey' = '|'" +
			")");

		/*tEnv.toAppendStream(tEnv.sqlQuery("SELECT CONCAT('cluster', cast(broker_id as VARCHAR(1))), CONCAT('topic', cast(topic_id as VARCHAR(1))) FROM source_test"), Row.class).print();
		env.execute("select job");*/
		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("INSERT INTO sink_test SELECT 'xixi',CONCAT('cluster', cast(broker_id as VARCHAR(1)))," +
			"CONCAT('topic', cast(topic_id as VARCHAR(1))) FROM source_test");
		stateSet.execute("execute unit test for diversion");

		while (true) {
			Thread.sleep(1000L);
		}
	}
}
