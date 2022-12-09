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

public class TestKafkaSource2 {
    @Test
    public void writeKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createSink = "CREATE TABLE kafka_sink_table (\n"
                + "   word string,\n"
                + "   num bigint,\n"
                + "   event_type string,\n"
                + "   app_id string, \n"
                + "   log_date string,\n"
                + "   log_hour string \n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'value.format' = 'csv',\n"
                + "  'topic' = 'ods_s_flink_kafka_source_test3',\n"
                + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092')";
        tableEnv.executeSql(createSink);

        DataStream<OrderInfo> sourceStream = env.addSource(
                new SourceFunction<OrderInfo>() {

                    final List<String> words = Arrays.asList(
                            "balabala",
                            "bilibili",
                            "wahaha",
                            "wakawaka",
                            "fuckyou");
                    final List<String> eventTypes = Arrays.asList(
                            "adiddas",
                            "biaoma",
                            "lining",
                            "newbalance",
                            "qiaodan");

                    @Override
                    public void run(SourceContext<OrderInfo> ctx) throws Exception {
                        while (true) {
                            String word = words.get(RandomUtils.nextInt(0, words.size()));
                            String eventType = eventTypes.get(RandomUtils.nextInt(0,
                                    eventTypes.size()));

                            OrderInfo orderInfo = new OrderInfo();
                            orderInfo.word = word;
                            orderInfo.num = RandomUtils.nextLong(100, 1000);
                            orderInfo.event_type = eventType;
                            orderInfo.app_id = String.valueOf(RandomUtils.nextLong(100000, 900000));
                            orderInfo.log_date = DateFormatUtils.format(new Date(), "yyyyMMdd");
                            orderInfo.log_hour = String.valueOf(Calendar
                                    .getInstance()
                                    .get(Calendar.HOUR_OF_DAY));

                            ctx.collect(orderInfo);
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
                        .column("word", DataTypes.STRING())
                        .column("num", DataTypes.BIGINT())
                        .column("event_type", DataTypes.STRING())
                        .column("app_id", DataTypes.STRING())
                        .column("log_date", DataTypes.STRING())
                        .column("log_hour", DataTypes.STRING())
                        .build());

        StreamStatementSet statementSet = tableEnv.createStatementSet();


        statementSet.addInsertSql(
                "insert into kafka_sink_table select word,num,event_type,app_id,log_date,log_hour from custom_source_table");
        statementSet.execute();

        tableEnv
                .executeSql(
                        "select word,num,event_type,app_id,log_date,log_hour from custom_source_table")
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
