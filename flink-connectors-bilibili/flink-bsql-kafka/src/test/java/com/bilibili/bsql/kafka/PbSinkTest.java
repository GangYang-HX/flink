/** Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved. */
package com.bilibili.bsql.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.ProtoToRowConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.bilibili.bsql.common.keys.TableInfoKeys.SABER_JOB_ID;
import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;

/** pb sink test */
public class PbSinkTest {

    @Test
    public void testKafkaSink() throws Exception {
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
        env.getConfig().setParallelism(10);

        tEnv.executeSql(
                "create function MY_TO_TIMESTAMP as 'com.bilibili.bsql.kafka.ToTimestamp' language java");
        tEnv.executeSql(
                "create function MY_TO_UNIXTIME as 'com.bilibili.bsql.kafka.ToUnixtime' language java");

		Table table =
			tEnv.fromValues(
				Arrays.asList(
//					Row.of("a", 1, 1636452123634L, Row.of("t1", 1)),
//					Row.of("b", 2, 1636445572634L, Row.of("t2", 1)),
					Row.of("h", 3, 1636457172634L, Row.of("t2", 1)),
					Row.of(null, 5, 1636457172634L, Row.of("t2", 1))))
//					Row.of("e", 4, 1636457172634L,Row.of("t6", 1))))
				.as("name", "num", "xtime", "row1");

        //        DataStream input = tEnv.toAppendStream(table, Row.class);
        //        input.print();
        tEnv.createTemporaryView("my_kafka_source", table);
        tEnv.executeSql(
                "CREATE TABLE pb_sink_test (\n"
                        + "  `name` STRING,\n"
                        + "  `num` INT,\n"
                        + "  `xtime` bigint,\n"
						+ "  `row1` Row(a varchar,b INTEGER),\n"
						+ "  primary key(name) not enforced\n"
                        + ") WITH (\n"
                        + "  'connector' = 'bsql-kafka10',\n"
                        + "  'topic' = 'zzj-test',\n"
                        +
                        //			"  'offsetReset' = 'latest',\n" +
                        "  'offsetReset' = 'earliest',\n"
                        +
                        //			"  'bootstrapServers' =
                        // '172.22.33.94:9092,172.22.33.99:9092,172.22.33.97:9092',\n" +
                        "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.KafkaSinkTestOuterClass$KafkaSinkTest',\n"
                        + "  'serializer' = 'proto_bytes',\n"
                        + "  'format' = 'protobuf'\n"
                        + ")");
        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql(
                "insert into pb_sink_test "
                        + "SELECT name, num, xtime,"
                        + "row1 "
                        + "FROM my_kafka_source");
        stateSet.execute("aaa");
        int count = 10;
        while (count > 0) {
            Thread.sleep(1000L);
            System.out.println("waiting insert");
            count--;
        }

        String sql = "SELECT * FROM pb_sink_test";
        Table result = tEnv.sqlQuery(sql);
        DataStream<Row> output = tEnv.toAppendStream(result, Row.class);
        output.print();

        env.execute();
    }

    @Test
    public void testKafkaSinkData() throws Exception {
        PbFormatConfig pbFormatConfig =
                new PbFormatConfig.PbFormatConfigBuilder()
                        .messageClassName(
                                "org.apache.flink.formats.protobuf.testproto.KafkaSinkTestOuterClass$KafkaSinkTest")
                        .build();
        List<RowType.RowField> rowFieldList =
                Arrays.asList(
                        new RowType.RowField("name", new VarCharType(), ""),
                        new RowType.RowField("num", new IntType(), ""),
                        new RowType.RowField("xtime", new BigIntType(), ""));
        RowType rowType = new RowType(rowFieldList);
        ProtoToRowConverter protoToRowConverter = new ProtoToRowConverter(rowType, pbFormatConfig);
        // 0A01611004189A9DCBA3D02F <=> (a,4,1636457172634);
        byte[] dataBytes =
                new byte[] {
                    0x0A,
                    0x01,
                    0x61,
                    0x10,
                    0x04,
                    0x18,
                    (byte) 0x9A,
                    (byte) 0x9D,
                    (byte) 0xCB,
                    (byte) 0xA3,
                    (byte) 0xD0,
                    0x2F
                };
        RowData rowData = protoToRowConverter.convertProtoBinaryToRow(dataBytes);
        System.out.println(rowData);
    }
}
