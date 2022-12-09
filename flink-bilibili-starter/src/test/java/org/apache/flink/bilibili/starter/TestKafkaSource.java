package org.apache.flink.bilibili.starter;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TestKafkaSource {
    @Test
    public void writeKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createSink = "CREATE TABLE kafka_sink_table (\n"
                + "   user_name string,\n"
                + "   age int,\n"
                + "   sex int, \n"
                + "   order_id string,\n"
                + "   movement string, \n"
                + "   log_date string,\n"
                + "   log_hour string \n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'value.format' = 'csv',\n"
                + "  'topic' = 'ods_s_flink_kafka_source_test2',\n"
                + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092')";
        tableEnv.executeSql(createSink);

        DataStream<UserInfo> sourceStream = env.addSource(
                new SourceFunction<UserInfo>() {

                    final List<String> userNames = Arrays.asList("yg", "hz", "lz", "sm", "hh");
                    final List<Integer> ages = Arrays.asList(22, 18, 33, 32, 20, 45);
                    final List<Integer> sexs = Arrays.asList(0, 1);
                    final List<String> movements = Arrays.asList(
                            "lanqiu",
                            "pingpang",
                            "zuqiu",
                            "bingqiu",
                            "paiqiu");

                    @Override
                    public void run(SourceContext<UserInfo> ctx) throws Exception {
                        while (true) {
                            String user_name = userNames.get(RandomUtils.nextInt(
                                    0,
                                    userNames.size()));
                            int age = ages.get(RandomUtils.nextInt(0, ages.size()));
                            int sex = sexs.get(RandomUtils.nextInt(0, sexs.size()));
                            String order_id = String.valueOf(RandomUtils.nextInt(100000, 900000));
                            String movement = movements.get(RandomUtils.nextInt(
                                    0,
                                    movements.size()));

                            UserInfo userInfo = new UserInfo();
                            userInfo.user_name = user_name;
                            userInfo.age = age;
                            userInfo.sex = sex;
                            userInfo.order_id = order_id;
                            userInfo.movement = movement;
                            userInfo.log_date = DateFormatUtils.format(new Date(), "yyyyMMdd");
                            userInfo.log_hour = String.valueOf(Calendar
                                    .getInstance()
                                    .get(Calendar.HOUR_OF_DAY));
                            ctx.collect(userInfo);
                            Thread.sleep(200L);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                });

        tableEnv.createTemporaryView("custom_source_table", sourceStream,
                Schema
                        .newBuilder()
                        .column("user_name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("sex", DataTypes.INT())
                        .column("order_id", DataTypes.STRING())
                        .column("movement", DataTypes.STRING())
                        .column("log_date", DataTypes.STRING())
                        .column("log_hour", DataTypes.STRING())
                        .build());

        StreamStatementSet statementSet = tableEnv.createStatementSet();


        statementSet.addInsertSql(
                "insert into kafka_sink_table select user_name,age,sex,order_id,movement,log_date,log_hour from custom_source_table");
        statementSet.execute();

        tableEnv
                .executeSql(
                        "select user_name,age,sex,order_id,movement,log_date,log_hour from custom_source_table")
                .print();
        env.execute();
    }

    @Test
    public void testPushDown() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createSource = "CREATE TABLE kafka_source_table (\n"
                + "   user_name string,\n"
                + "   age int,\n"
                + "   sex int, \n"
                + "   order_id string,\n"
                + "   movement string \n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'ods_s_flink_kafka_source_test',\n"
                + "  'properties.group.id' = 'testGroup',\n"
                + "  'format' = 'csv',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092')";
        tableEnv.executeSql(createSource);


        tableEnv.executeSql("select user_name,movement from kafka_source_table").print();
        env.execute("test_consumer_source");
    }
}
