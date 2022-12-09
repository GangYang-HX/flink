package org.apache.flink.formats;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

public class DeserializationTest {
	@Test
	public void testDeserialization() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance()
				// Watermark is only supported in blink planner
				.useBlinkPlanner()
				.inStreamingMode()
				.build());

		tEnv.executeSql("CREATE TABLE source (\n" +
			"  map1 map<string, string>,\n" +
			"  my_field array<int>,\n" +

			"  map2 map<string, row<a int, b bigint>>\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'meimeizi_test',\n" +
			//"  'offsetReset' = 'earliest',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTestOuterClass$MapTest',\n" +
			"  'format' = 'protobuf'\n" +
			")");
		Table table = tEnv.sqlQuery("select * from source");
		tEnv.toAppendStream(table, Row.class).print();
		env.execute("do");
	}
}
