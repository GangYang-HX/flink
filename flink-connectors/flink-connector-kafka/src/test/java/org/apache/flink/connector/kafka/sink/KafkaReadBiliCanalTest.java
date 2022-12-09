package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.bilicanal.BiliCanalJsonDeserializationSchema;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** Test for {@link BiliCanalJsonDeserializationSchema}. */
public class KafkaReadBiliCanalTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReadBiliCanalTest.class);

    private static ThreadLocal<ByteArrayOutputStream> byteStreamThreadLocal =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream());
    private static ThreadLocal<BinaryEncoder> encoderThreadLocal =
            ThreadLocal.withInitial(() -> null);

    private static ThreadLocal<SpecificDatumWriter<AvroFlumeEvent>> writerThreadLocal =
            ThreadLocal.withInitial(() -> new SpecificDatumWriter<>(AvroFlumeEvent.class));

    private static final byte[] MAGIC_HEAD = new byte[] {0x01, 0x01, 0x01, 0x01};
    private String topic;
    private Admin admin;
    private Properties kafkaProps;

    private String insertRecord =
            "{\"action\":\"insert\",\"schema\":\"live_audit\","
                    + "\"table\":\"audit_statistics_record_count\","
                    + "\"pk_names\":\"id\",\"new\":{\"business_type\":1,"
                    + "\"ctime\":\"2022-05-25 23:14:08\",\"id\":24331475,"
                    + "\"mtime\":\"2022-05-25 23:14:08\",\"record_count\":1,"
                    + "\"source_data_create_at\":\"2022-05-25 23:14:08\"},"
                    + "\"msec\":1653491648139,\"seq\":6935246633756000270,"
                    + "\"canal_pos_str\":\"433452914568855753\"}";

    private String updateRecord =
            "{\"action\":\"update\",\"schema\":\"live_audit\","
                    + "\"table\":\"audit_statistics_record_count\","
                    + "\"pk_names\":\"id\",\"old\":{\"business_type\":7,"
                    + "\"ctime\":\"2022-05-10 01:37:27\",\"id\":23469611,"
                    + "\"mtime\":\"2022-05-10 01:37:27\",\"record_count\":139,"
                    + "\"source_data_create_at\":\"2022-05-10 01:00:00\"},"
                    + "\"new\":{\"business_type\":17,\"ctime\":\"2022-05-10 01:37:43\","
                    + "\"id\":23469611,\"mtime\":\"2022-05-10 01:37:43\","
                    + "\"record_count\":140,\"source_data_create_at\":\"2022-05-10 01:00:00\"},"
                    + "\"msec\":1652117863736,\"seq\":6929484564339359744,"
                    + "\"canal_pos_str\":\"433092785231101988\"}";

    private String deleteRecord =
            "{\"action\":\"delete\",\"schema\":\"reply\","
                    + "\"table\":\"reply_control\",\"pk_names\":\"rpid,kind\","
                    + "\"new\":{\"bucket\":1,\"ctime\":\"2022-05-26 12:08:57\","
                    + "\"delta\":0.9,\"factor\":0.5,"
                    + "\"kind\":4,"
                    + "\"meta\":\"{\\\"creator\\\":\\\"govern\\\"}\","
                    + "\"mtime\":\"2022-05-26 12:08:57\","
                    + "\"offset\":20,\"oid\":256615129,\"order\":8,"
                    + "\"rpid\":112957061104,\"state\":1,\"type\":2},"
                    + "\"msec\":1653538186495,\"seq\":6935441829768724483,"
                    + "\"canal_pos_str\":\"433465114321224359\"}";

    @Before
    public void init() {
        topic = "canal2kafka_test_shin";
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "10.221.51.174:9092");
        kafkaProps.put("group.id", "test_shin"); // consumer group
        kafkaProps.put(
                "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProps.put(
                "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        admin = KafkaAdminClient.create(kafkaProps);
    }

    /**
     * send test data to UAT Kafka.
     *
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void sendAvroBinlogData() throws ExecutionException, InterruptedException {
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(kafkaProps);
        try {
            // send record
            int i = 1;
            while (i < 3) {
                byte[] updateavrobyte = serialize(insertRecord);
                ProducerRecord<byte[], byte[]> updateRecord =
                        new ProducerRecord<byte[], byte[]>(topic, updateavrobyte);
                producer.send(updateRecord);
                i++;
                // send a record every one second
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    /**
     * serialize the origin data to inner record format bili-canal.
     *
     * @param body the body
     * @return the byte [ ]
     * @throws IOException the io exception
     */
    public byte[] serialize(String body) throws IOException {
        long now = System.currentTimeMillis();
        String nowStr = String.valueOf(now);
        Map<String, String> header = new HashMap<>();
        header.put("logId", "012632");
        header.put("version", "1.1");
        header.put("ctime", nowStr);
        header.put("timestamp_global", nowStr);
        header.put("utime", nowStr);

        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream = byteStreamThreadLocal.get();
        byteArrayOutputStream.reset();
        SpecificDatumWriter<AvroFlumeEvent> writer = writerThreadLocal.get();
        BinaryEncoder encoder = encoderThreadLocal.get();

        AvroFlumeEvent avroFlumeEvent =
                new AvroFlumeEvent(toCharSeqMap(header), ByteBuffer.wrap(body.getBytes()));
        encoder = EncoderFactory.get().directBinaryEncoder(byteArrayOutputStream, encoder);
        try {
            writer.write(avroFlumeEvent, encoder);
            encoder.flush();
            byte[] avroFlumeEventBytes = byteArrayOutputStream.toByteArray();
            bytes = new byte[MAGIC_HEAD.length + avroFlumeEventBytes.length];
            System.arraycopy(MAGIC_HEAD, 0, bytes, 0, MAGIC_HEAD.length);
            System.arraycopy(
                    avroFlumeEventBytes, 0, bytes, MAGIC_HEAD.length, avroFlumeEventBytes.length);
        } catch (IOException e) {
            LOG.error("serialize event error,event:{}", avroFlumeEvent.toString(), e);
        }
        return bytes;
    }

    private Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
        Map<CharSequence, CharSequence> charSeqMap = new HashMap<>(stringMap.size());
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            charSeqMap.put(entry.getKey(), entry.getValue());
        }
        return charSeqMap;
    }

    /**
     * test delete stream data.
     *
     * @throws Exception the exception
     */
    @Test
    public void test_bilicanal_sql_delete() throws Exception {
        String[] argss = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(argss);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // set up execution environment
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) { // use blink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) { // use flink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else {
            System.err.println(
                    "The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', "
                            + "where planner (it is either flink or blink, "
                            + "and the default is blink) indicates whether the "
                            + "example uses flink planner or blink planner.");
            return;
        }

        tEnv.executeSql(
                "CREATE TABLE STD_LOG (\n"
                        + "  seq bigint,\n"
                        + " `rpid` bigint,\n"
                        + "  `oid` bigint,\n"
                        + "  `type` bigint,\n"
                        + "  `kind` bigint,\n"
                        + "  `state` bigint,\n"
                        + "  `bucket` bigint,\n"
                        + "  `order` bigint,\n"
                        + "  `factor` double,\n"
                        + "  `meta` string,\n"
                        + "  `ctime` string,\n"
                        + "  `mtime` string,\n"
                        + "  `delta` double\n"
                        + ")WITH(\n"
                        + "   'connector' = 'print'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_kafka_1(\n"
                        + "  seq BIGINT METADATA from 'value.__seq__' VIRTUAL,\n"
                        + " `rpid` bigint,\n"
                        + "  `oid` bigint,\n"
                        + "  `type` bigint,\n"
                        + "  `kind` bigint,\n"
                        + "  `state` bigint,\n"
                        + "  `bucket` bigint,\n"
                        + "  `order` bigint,\n"
                        + "  `factor` double,\n"
                        + "  `meta` string,\n"
                        + "  `ctime` string,\n"
                        + "  `mtime` string,\n"
                        + "  `delta` double\n"
                        + ") WITH(\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092',\n"
                        + "  'topic' = 'canal2kafka_test_shin',\n"
                        + "  'properties.group.id' = 'test_new', \n"
                        + "  'connector' = 'kafka',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'format' = 'bili-canal'\n"
                        + ")");

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql(
                "INSERT INTO\n"
                        + "  STD_LOG\n"
                        + "SELECT\n"
                        + "  `seq`,\n"
                        + " `rpid`,\n"
                        + "  `oid`,\n"
                        + "  `type`,\n"
                        + "  `kind`,\n"
                        + "  `state`,\n"
                        + "  `bucket`,\n"
                        + "  `order`,\n"
                        + "  `factor`,\n"
                        + "  `meta`,\n"
                        + "  `ctime`,\n"
                        + "  `mtime`,\n"
                        + "  `delta`\n"
                        + "FROM\n"
                        + "  sink_kafka_1");
        stateSet.execute();
        while (true) {
            Thread.sleep(1000L);
        }
    }

    /**
     * 测试insert和update流数据.
     *
     * @throws Exception the exception
     */
    @Test
    public void test_bilicanal_sql_insert_and_update() throws Exception {
        String[] argss = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(argss);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // set up execution environment
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) { // use blink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) { // use flink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else {
            System.err.println(
                    "The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', "
                            + "where planner (it is either flink or blink, "
                            + "and the default is blink) indicates whether the "
                            + "example uses flink planner or blink planner.");
            return;
        }

        tEnv.executeSql(
                "CREATE TABLE STD_LOG (\n"
                        + "  seq bigint,\n"
                        + " `id` bigint,\n"
                        + "  `business_type` bigint,\n"
                        + "  `source_data_create_at` string,\n"
                        + "  `record_count` bigint,\n"
                        + "  `ctime` string,\n"
                        + "  `mtime` string\n"
                        + ")WITH(\n"
                        + "   'connector' = 'print'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_kafka_1(\n"
                        + "  seq bigint METADATA from 'value.__seq__' VIRTUAL,\n"
                        + "`id` bigint,\n"
                        + "  `business_type` bigint,\n"
                        + "  `source_data_create_at` string,\n"
                        + "  `record_count` bigint,\n"
                        + "  `ctime` string,\n"
                        + "  `mtime` string\n"
                        + ") WITH(\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092',\n"
                        + "  'topic' = 'canal2kafka_test_shin',\n"
                        + "  'properties.group.id' = 'test_new', \n"
                        + "  'connector' = 'kafka',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'format' = 'bili-canal'\n"
                        + ")");

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql(
                "INSERT INTO\n"
                        + "  STD_LOG\n"
                        + "SELECT\n"
                        + "  `seq`,\n"
                        + "  `id`,\n"
                        + "  `business_type`,\n"
                        + "  `source_data_create_at`,\n"
                        + "  `record_count`,\n"
                        + "  `ctime`,\n"
                        + "  `mtime`\n"
                        + "FROM\n"
                        + "  sink_kafka_1");
        stateSet.execute();
        while (true) {
            Thread.sleep(1000L);
        }
    }
}
