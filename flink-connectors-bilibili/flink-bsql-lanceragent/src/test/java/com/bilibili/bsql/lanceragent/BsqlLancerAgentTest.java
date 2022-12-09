package com.bilibili.bsql.lanceragent;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlLancerAgentTest.java
 * @description This is the description of BsqlLancerAgentTest.java
 * @createTime 2020-11-10 18:58:00
 */
public class BsqlLancerAgentTest {

    @Test
    public void sinkTest() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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


        String sql = "CREATE TABLE wxm_test (`userss` VARCHAR, `product` BIGINT, `amount` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
        tEnv.executeSql(sql);

        Table table2 = tEnv.sqlQuery("select * from wxm_test");

        String clickHouseSql = "CREATE TABLE lancer_agent_sink_test (info_index VARCHAR,courier_id BIGINT,city_id BIGINT" +
                ") WITH (" +
                "'connector'='bsql-lanceragent'," +
                "'logId'='001538'," +
                "'bsql-delimit.delimiterKey'='x'," +
                "'parallelism'='1'" +
                ")";
        tEnv.executeSql(clickHouseSql);

        Table clickHouseTable = tEnv.sqlQuery("select `userss` as info_index,`product` as courier_id,`amount` as city_id from " + table2);

        tEnv.executeSql("INSERT INTO lancer_agent_sink_test SELECT info_index,courier_id,city_id FROM " + clickHouseTable);
        tEnv.toRetractStream(table2, Row.class).print("===== ");
        env.execute("");
    }
}
