package com.bilibili.bsql.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.*;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

public class DeserializationTest {
    @Test
    public void testDeserializationTT() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
            env,
            EnvironmentSettings.newInstance()
                // Watermark is only supported in blink planner
                .useBlinkPlanner()
                .inStreamingMode()
                .build());

        tEnv.executeSql("CREATE TABLE source (\n"
                + "  `name` STRING,\n"
                + "  `num` INT,\n"
                + "  `xtime` bigint,\n"
                + "  `row1` Row(a varchar,b INT)\n"+
            ") WITH (\n" +
            "  'connector' = 'bsql-kafka10',\n" +
            "  'topic' = 'zzj-test',\n" +
            "  'offsetReset' = '1642749641000',\n" +
            "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
            "  'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.KafkaSinkTestOuterClass$KafkaSinkTest',\n" +
            "  'format' = 'protobuf',\n" +
            "  'protobuf.add-default-value' = 'false',\n" +
            "  'protobuf.ignore-parse-errors' = 'true',\n" +
            "  'protobuf.ignore-null-rows' = 'false'" +

            ")");
        Table table = tEnv.sqlQuery("select * from source");
        tEnv.toAppendStream(table, Row.class).print();
        env.execute("do");
    }

    @Test
    public void testDeserialization2() throws Exception {
        sendKafkaData(false);
        consumeKafkaData(false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
        tabEnv.registerFunction("to_unixtime",new ToUnixtime());
        tabEnv.executeSql("CREATE TABLE source_(\n" +
                "  f1 varchar,\n" +
                "  f2 varchar,\n" +
                "  f3 varchar\n" +
                ") WITH(\n" +
                "  'connector' = 'bsql-kafka10',\n" +
                "  'offsetReset' = 'earliest',\n" +
                "  'topic' = 'lazy_deserializer',\n" +
                "  'bootstrapServers' = '10.221.51.174:9092 10.221.50.145:9092 10.221.50.131:9092',\n" +
                "  'bsql-delimit.delimiterKey' = '|'\n" +
                ")");

        Table queryRet = tabEnv.sqlQuery("select f3,f2 from source_");

        tabEnv.toAppendStream(queryRet, Row.class).addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) {
                System.out.println(value);
            }
        });

        env.execute("lazy_deserializer");

        Thread.sleep(3600_000);
    }

    private void sendKafkaData(boolean execute) {

        if (!execute) {
            return;
        }
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.221.51.174:9092 10.221.50.145:9092 10.221.50.131:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        Producer<byte[], byte[]> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            String val = "f1_" + i + "|f2_" + i + "|f3_" + i;
            ProducerRecord<byte[], byte[]> record = new ProducerRecord("lazy_deserializer", val.getBytes());
            try {
                producer.send(record).get();
                System.out.println("sendOk:" + val);
            } catch (Exception e) {
                System.out.println("sendFailed:" + val);
            }
        }
    }

    private void consumeKafkaData(boolean execute) {

        if (!execute) {
            return;
        }

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.221.51.174:9092 10.221.50.145:9092 10.221.50.131:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lazy_deserializer");
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("lazy_deserializer"));
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(500);
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<byte[], byte[]> record = iterator.next();
                System.out.println(new String(record.value()));
            }
        }
    }

    @Test
    public void testLancerDeserializationSchemaWrapper() throws InterruptedException {
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

		tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
			"  `value` STRING,\n" +
			"  `number` INT,\n" +
			"  `timestamp` TIMESTAMP(3) METADATA,\n" +
			" `timestamp-type` string METADATA,\n" +
			"  `headers` MAP<varchar,bytes> METADATA,\n" +
			"  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
			"WATERMARK  FOR ts as ts- INTERVAL '0.1' SECOND \n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'lancer_udf_test_4',\n" +
			"  'offsetReset' = 'latest',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'bsql-delimit.delimiterKey' = '\u0001',   -- declare a format for this system\n" +
			"  'bsql-delimit.useLancerFormat' = 'true', \n" +
			"  'bsql-delimit.traceId' = '123456', \n" +
			"  'bsql-delimit.sinkDest' = 'KAFKA'" +
			")");

//        tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
//            "  `value` STRING,\n" +
//            "  `timestamp` TIMESTAMP(3) METADATA,\n" +
//            "  `timestamp-type` string METADATA,\n" +
//            "  `headers` MAP<varchar,bytes> METADATA,\n" +
//            "   `headers['ctime']` TIMESTAMP(3) FROM METADATA `headers`,\n"+
//            "   WATERMARK  FOR `headers['ctime']` AS `headers['ctime']`- INTERVAL '0.1' SECOND \n" +
//            ") WITH (\n" +
//            "  'connector' = 'bsql-kafka10',\n" +
//            "  'topic' = 'kafka_proxy_test_topic_900000',\n" +
//            "  'offsetReset' = 'earliest',\n" +
//            "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
//            "  'bsql-delimit.delimiterKey' = ',',   -- declare a format for this system\n" +
//            "  'bsql-delimit.useLancerFormat' = 'true', \n" +
//            "  'bsql-delimit.traceId' = '123456', \n" +
//            "  'bsql-delimit.sinkDest' = 'KAFKA'" +
//            ")");


		tEnv.executeSql("CREATE TABLE sink_test (\n" +
			"  `value` STRING,\n" +
			"  `number` INT,\n" +
			"  `timestamp` TIMESTAMP(3) METADATA,\n" +
			"  `headers` MAP<STRING, BYTES> METADATA\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-kafka10',\n" +
			"  'topic' = 'kafka_proxy_test_topic_900001',\n" +
			"  'partitionStrategy' = 'BUCKET',\n" +
			"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
			"  'bsql-delimit.delimiterKey' = '\u0001'" +
			")");


        /*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
        env.execute();*/

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test " +
            "SELECT " +
            "a.`value`,"+
			"a.`number`,"+
            "a.`timestamp`," +
            "a.`headers`" +
            " FROM my_kafka_test as a");
        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }
}
