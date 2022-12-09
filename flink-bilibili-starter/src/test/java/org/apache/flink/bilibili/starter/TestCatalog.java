package org.apache.flink.bilibili.starter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.bilibili.catalog.BilibiliCatalog;
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
 * @Author: JinZhengyu
 * @Date: 2022/7/29 下午4:01
 */
public class TestCatalog {

    @Test
    public void sinkWorkability1() throws Exception {
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
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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

        tEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));

        tEnv.executeSql("create table source WITH (" +
                "'offsetRest' ='latest'," +
                "'bsql-delimit.delimiterKey' = ','" +
                ") like Kafka_1.r_ods.tpc_test_source");

        tEnv.executeSql("create table sink WITH (" +
                "'offsetRest' ='latest'," +
                "'bsql-delimit.delimiterKey' = ','" +
                ") like Kafka_1.r_ods.tpc_test_sink");


        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink " +
                "SELECT " +
                "a.col1," +
                "a.col2" +
                " FROM source as a");
        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }


    @Test
    public void sinkWorkability2() throws Exception {
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
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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
        tEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));
        tEnv.executeSql("create table hive_side WITH (" +
                "'collector' ='bsql-hive-side'," +
                "'join-key' = 'col1'" +
                ") like Hive1.bili_dim.xinxue_test_dim");
        String sql = "CREATE TABLE source (`col1` bigint, ts AS localtimestamp,WATERMARK FOR ts AS ts - INTERVAL '8' HOUR) " +
                "WITH ('connector'='bsql-datagen','rows-per-second'='1','fields.col1.min' = '1','fields.col1.max' = '10')";
        tEnv.executeSql(sql);

        tEnv.executeSql("CREATE TABLE print_sink (\n"
                + "  col1 bigint,\n"
                + "  col2 STRING,\n"
                + "  col3 STRING,\n"
                + "  col4 STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print',\n"
                + "  'print-identifier' = 'printa'\n"
                + ")");

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO print_sink " +
                "SELECT " +
                "a.col1," +
                "b.col2" +
                " FROM source as a left join hive_side FOR SYSTEM_TIME AS OF now() AS b ON a.col1 = b.col1 ");
        stateSet.execute("aaa");
        while (true) {
            Thread.sleep(1000L);
        }
    }


}
