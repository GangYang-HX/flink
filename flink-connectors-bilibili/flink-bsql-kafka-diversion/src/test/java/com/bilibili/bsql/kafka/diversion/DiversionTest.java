package com.bilibili.bsql.kafka.diversion;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;

import java.util.Objects;

import scala.concurrent.duration.Duration;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

/** DiversionTest. */
public class DiversionTest {
    @Test
    public void testDiversionProducer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(10000);

        tEnv.executeSql(
                "create function BrokerUdf as 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf' language java");
        tEnv.executeSql(
                "create function TopicUdf as 'com.bilibili.bsql.kafka.diversion.udf.TopicUdf' language java");

        tEnv.executeSql(
                "CREATE TABLE source_test (\n"
                        + "  broker_id BIGINT,\n"
                        + "  topic_id BIGINT\n"
                        + ") WITH(\n"
                        + "  'connector' = 'bsql-datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'fields.broker_id.min' = '1',\n"
                        + "  'fields.broker_id.max' = '2',\n"
                        + "  'fields.topic_id.min' = '1',\n"
                        + "  'fields.topic_id.max' = '2'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_test (\n"
                        + "  `timestampcc` STRING,\n"
                        + "  `broker_arg` STRING,\n"
                        + "  `headers` MAP < STRING,BYTES > METADATA,\n"
                        + "  `topic_arg` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'bsql-kafka-diversion',\n"
                        + "  'properties.bootstrap.servers' = 'BrokerUdf(broker_arg)',\n"
                        + "  'bootstrapServers.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf',\n"
                        + "  'topic' = 'TopicUdf(timestamp-type,timestamp-type)',\n"
                        + "  'topic.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.TopicUdf',\n"
                        + "  'udf.cache.minutes' = '1' ,\n"
                        + "  'format' = 'csv'"
                        + ")");

        /*tEnv.toAppendStream(tEnv.sqlQuery("SELECT CONCAT('cluster', cast(broker_id as VARCHAR(1))), CONCAT('topic', cast(topic_id as VARCHAR(1))) FROM source_test"), Row.class).print();
        env.execute("select job");*/
        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql(
                "INSERT INTO sink_test "
                        + "SELECT  'xixi',CONCAT('cluster', cast(broker_id as VARCHAR(1))),Map['key',ENCODE('value','UTF-8')],"
                        + "CONCAT('topic', cast(topic_id as VARCHAR(1))) FROM source_test");
        stateSet.execute("execute unit test for diversion");

        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void testKafka2Kafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(100);
        //        tEnv.executeSql("create function BrokerUdf as
        // 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf' language java");
        //        tEnv.executeSql("create function TopicUdf as
        // 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf.TopicUdf' language java");

        tEnv.executeSql(
                "CREATE TABLE source_test ( \n"
                        + "  `body` bytes,\n"
                        + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                        //                        + "  `timestamp-type` string METADATA,\n"
                        + "  `headers` MAP < varchar,bytes > METADATA,\n"
                        + "  WATERMARK FOR `timestamp` as `timestamp` - INTERVAL '0.1' SECOND ) WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'lancer_udf_test_4',\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'format' = 'raw'\n"
                        + "  )");

        // diversion kafka
        //        tEnv.executeSql(
        //                "CREATE TABLE sink_test (\n"
        //                        + "  `body` bytes,\n"
        //                        + "  `timestamp` TIMESTAMP(3) METADATA,\n"
        //                        + "  `timestamp-type` string METADATA,\n"
        //                        + "  `headers` MAP < varchar,BYTES > METADATA\n"
        //                        + ") WITH (\n"
        //                        + "  'connector' = 'bsql-kafka10-diversion',\n"
        //                        + "  'properties.bootstrap.servers' =
        // 'BrokerUdf(timestamp-type)',\n"
        //                        + "  'bootstrapServers.udf.class' =
        // 'com.bilibili.bsql.kafka.diversion.udf.BrokerUdf',\n"
        //                        + "  'topic' = 'TopicUdf(timestamp-type,timestamp-type)',\n"
        //                        + "  'topic.udf.class' =
        // 'com.bilibili.bsql.kafka.diversion.udf.TopicUdf',\n"
        //                        +
        //                        // "  'exclude.udf.field' = 'false',\n" +
        //                        "  'udf.cache.minutes' = '1' ,\n"
        //                         + "  'format' = 'csv'" +
        //                        ")");
        tEnv.executeSql(
                "CREATE TABLE sink_test (\n"
                        + "  `body` bytes,\n"
                        + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                        //                        + "  `timestamp-type` string METADATA,\n"
                        + "  `headers` MAP < varchar,BYTES > METADATA\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'topic' = 'cluster1',\n"
                        + "  'format' = 'csv'"
                        + ")");

        StatementSet stateSet = tEnv.createStatementSet();

        stateSet.addInsertSql(
                "INSERT INTO\n"
                        + "  sink_test\n"
                        + "SELECT\n"
                        + "  a.body,\n"
                        + "  a.`timestamp`,\n"
                        //                + "  a.`timestamp-type`,\n"
                        + "  a.`headers`\n"
                        + "FROM\n"
                        + "  source_test a");
        stateSet.execute("execute unit test for diversion");

        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void lancerLocalDBTest() throws Exception {
        Duration duration = Duration.create("30 s");
        System.err.println(duration);

        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        Configuration configuration = new Configuration();
        configuration.setString(String.valueOf(TABLE_EXEC_SOURCE_IDLE_TIMEOUT), "30 sec");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(2);
        env.enableCheckpointing(1_000);
        env.setRestartStrategy(RestartStrategies.noRestart());
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
                            + "where planner (it is either flink or blink, and the default is blink) indicates whether the "
                            + "example uses flink planner or blink planner.");
            return;
        }

        String sourceSql =
                "-- creates a mysql cdc table source\n"
                        + "CREATE TABLE mysql_binlog (\n"
                        + " host_name STRING METADATA FROM 'host_name' VIRTUAL,\n"
                        + " db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                        + " table_name STRING METADATA  FROM 'table_name' VIRTUAL,\n"
                        + " operation_ts TIMESTAMP_LTZ(3) METADATA  FROM 'op_ts' VIRTUAL,\n"
                        + " sequence BIGINT METADATA  FROM 'sequence' VIRTUAL,\n"
                        + " id INT NOT NULL,\n"
                        + " filed_list BYTES NOT NULL,\n"
                        + " PRIMARY KEY(id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'mysql-cdc',\n"
                        + " 'server-time-zone' = 'Asia/Shanghai',\n"
                        + " 'hostname' = 'localhost',\n"
                        + " 'port' = '3306',\n"
                        + " 'debezium.format' = 'bytes',\n"
                        + " 'username' = 'root',\n"
                        + " 'password' = 'weizefeng',\n"
                        + " 'database-name' = 'cdc_test',\n"
                        + " 'table-name' = 'tm_test.*'\n"
                        + ")";

        tEnv.executeSql(sourceSql);

        tEnv.toChangelogStream(tEnv.sqlQuery("SELECT * FROM mysql_binlog")).print();
        env.execute();
    }
}
