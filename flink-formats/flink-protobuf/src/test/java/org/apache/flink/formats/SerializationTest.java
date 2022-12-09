package org.apache.flink.formats;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SerializationTest {


	@Test
	public void testSerialization() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance()
				// Watermark is only supported in blink planner
				.useBlinkPlanner()
				.inStreamingMode()
				.build());
		//source
		Map<String, String> map1_1 = new HashMap<>();
		map1_1.put("map1_1","1");
		Map<String, String> map1_2 = new HashMap<>();
		map1_2.put("map1_2","2");
		Map<String, String> map1_3 = new HashMap<>();
		map1_3.put("map1_3","3");

		Map<String, InnerMessageTest> map2_1 = new HashMap<>();
		map2_1.put("map2_1", new InnerMessageTest(1,1));
		Map<String, InnerMessageTest> map2_2 = new HashMap<>();
		map2_2.put("map2_2", new InnerMessageTest(2,2));
		Map<String, InnerMessageTest> map2_3 = new HashMap<>();
		map2_3.put("map2_3", new InnerMessageTest(3,3));
		DataStream<Row> sourceStream = env.fromElements(Row.of(1, map1_1, map2_1),
			Row.of(2, map1_2, map2_2),
			Row.of(3, map1_3, map2_3)).returns(Types.ROW(Types.INT,Types.MAP(Types.STRING,Types.STRING),Types.MAP(Types.STRING,Types.POJO(InnerMessageTest.class))));
		Table tableSource = tEnv.fromDataStream(sourceStream).as("a", "map1", "map2");
		System.out.println(Arrays.toString(tableSource.getSchema().getFieldDataTypes()));
		tEnv.createTemporaryView("source", tableSource);
		//sink
		tEnv.executeSql("CREATE TABLE sink (\n" +
			"  a int,\n" +
			"  map1 map<string, string>,\n" +
			"  map2 map<string, row<a int, b bigint>>\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'meimeizi_test',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest',\n" +
			"  'format' = 'protobuf'\n" +
			")");
		tEnv.executeSql("insert into sink select * from source");

		tEnv.execute("ss");
	}

	public static class InnerMessageTest implements Serializable {
		private int a;
		private long b;

		public InnerMessageTest(){}

		public InnerMessageTest(int a, long b) {
			this.a = a;
			this.b = b;
		}

		public int getA() {
			return a;
		}

		public InnerMessageTest setA(int a) {
			this.a = a;
			return this;
		}

		public long getB() {
			return b;
		}

		public InnerMessageTest setB(long b) {
			this.b = b;
			return this;
		}
	}

}
