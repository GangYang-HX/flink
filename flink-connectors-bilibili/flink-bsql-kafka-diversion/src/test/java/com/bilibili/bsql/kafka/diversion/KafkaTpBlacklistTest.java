package com.bilibili.bsql.kafka.diversion;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.Objects;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

/**
 * @Description
 * @Author weizefeng
 * @Date 2022/3/8 11:12
 **/
public class KafkaTpBlacklistTest {
    @Test
    public void sinkWorkability() throws Exception {
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

        tEnv.executeSql("CREATE TABLE source_test (\n" +
            "  id BIGINT,\n" +
            "  topic_name string metadata from 'topic', \n" +
            " `partition_id` INT METADATA FROM 'partition',\n" +
            "  mock_time TIMESTAMP(3),\n" +
            "  `offset` BIGINT METADATA,\n" +
            "  `timestamp` TIMESTAMP(3) METADATA,\n" +
            " `timestamp-type` string METADATA,\n" +
            "  computed_column AS PROCTIME(),\n" +
            "  `headers` MAP<STRING, BYTES> METADATA,\n" +
            "  WATERMARK FOR mock_time AS mock_time - INTERVAL '5' SECOND\n" +
            ") WITH(\n" +
            "  'connector' = 'bsql-kafka10',\n" +
            "  'topic' = 'kafka_proxy_test_topic_900000',\n" +
            "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
            "  'bsql-delimit.delimiterKey' = ',',\n" +
            "  'blacklist.enable'='true',\n" +
            "  'blacklist.zk.host'='localhost:2181',\n" +
            "  'blacklist.lag.times'= '8'" +
            ")");


        tEnv.executeSql("CREATE TABLE sink_test (\n" +
            "  id BIGINT,\n" +
            "  topic_name string ,\n" +
            " `partition_id` INT ,\n" +
            "  mock_time TIMESTAMP(3),\n" +
            "  `offset` BIGINT ,\n" +
            "  `timestamp` TIMESTAMP(3),\n" +
            " `timestamp-type` string,\n" +
            "  computed_column TIMESTAMP(3),\n" +
            "  `headers` MAP<STRING, BYTES>\n" +
            ") WITH (\n" +
            "  'connector' = 'bsql-kafka10-diversion',\n" +
            "  'topic' = 'kafka_proxy_test_topic_900000',\n" +
            "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
            "  'topic.udf.class' = 'com.bilibili.bsql.kafka.diversion.udf.TopicUdf',\n" +
            "  'bsql-delimit.delimiterKey' = ',',\n" +
            "  'blacklist.enable'='true',\n" +
            "  'blacklist.zk.host'='localhost:2181'" +
            ")");


        /*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
        env.execute();*/

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test " +
            "SELECT " +
            "a.id," +
            "a.topic_name," +
            "a.partition_id," +
            "a.mock_time," +
            "a.`offset`," +
            "a.`timestamp`," +
            "a.`timestamp-type`," +
            "a.computed_column," +
            "a.`headers`" +
            " FROM source_test as a");
        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }
}
