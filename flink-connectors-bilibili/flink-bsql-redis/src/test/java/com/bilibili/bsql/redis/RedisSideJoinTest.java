package com.bilibili.bsql.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import scala.concurrent.duration.Duration;

public class RedisSideJoinTest {
    @Test
    public void test() throws InterruptedException {
        Duration duration = Duration.create("40 s");
        System.err.println(duration);


        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);

        StreamTableEnvironment tEnv;
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql("CREATE TABLE source_test (\n" +
                "  id1 BIGINT,\n" +
                "  id2 BIGINT\n" +
                ") WITH(\n" +
                "  'connector' = 'bsql-datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.id1.min' = '1',\n" +
                "  'fields.id1.max' = '2',\n" +
                "  'fields.id2.min' = '3',\n" +
                "  'fields.id2.max' = '4'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE redis_side (\n" +
                "  key VARCHAR,\n" +
                "  val Array<VARBINARY>,\n" +
                "  PRIMARY KEY (key) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'asynctimeout' = '1000',\n" +
                "  'redistype' = '3',\n" +
                "  'delimitKey' = ',',\n" +
                "  'asynccapacity' = '20000',\n" +
                "  'connector' = 'bsql-redis',\n" +
                "  'url' = '172.23.47.13:7115'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_test (\n" +
                "  key1 BIGINT,\n" +
                "  key2 BIGINT,\n" +
                "  val Array<VARBINARY>\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-log'\n" +
                ")");

        StatementSet stateSet =tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test " +
                "SELECT a.id1,a.id2, val FROM source_test a left JOIN " +
                "redis_side FOR SYSTEM_TIME AS OF now() AS b ON " +
                "concat(concat(concat('key', cast(a.id1 as VARCHAR(1))), ','), concat('key', cast(a.id2 as VARCHAR(1)))) = b.key");


        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }
}
