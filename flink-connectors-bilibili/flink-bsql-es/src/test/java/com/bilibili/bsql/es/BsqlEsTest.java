package com.bilibili.bsql.es;

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
 * @className BsqlEsTest.java
 * @description This is the description of BsqlEsTest.java
 * @createTime 2020-10-28 14:50:00
 */
public class BsqlEsTest {

    @Test
    public void esSinkTest() throws Exception {
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

        tEnv.executeSql("create function jsonstr_to_row as 'com.bilibili.bsql.es.JsonStrToRow' language java");

        tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
                "  `commodity_id` bigint,\n" +
                "  `commodity_name` VARCHAR,\n" +
                "  `price` double\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-datagen',\n" +
                "  'rows-per-second' = '10'\n" +
//                "  'topic' = 'flink_sql_kafka_sink',\n" +
//                "  'offsetReset' = 'latest',\n" +
//                "  'bootstrapServers' = '127.0.0.1:9092',\n" +
//                "  'parallelism' = '1',\n" +
//                "  'bsql-delimit.delimiterKey' = '|'   -- declare a format for this system\n" +
                ")");

        Table table2 = tEnv.sqlQuery("select * from my_kafka_test");

        String clickHouseSql = "CREATE TABLE es_flink_sink_test (" +
//                "aaa ROW<s STRING>," +
//                "bbb VARCHAR," +
//                "`@timestamp` TIMESTAMP" +
			"  `commodity_id` bigint,\n" +
			"  `commodity_name` VARCHAR,\n" +
			"  `price` double\n" +
                ") WITH (" +
                "'connector'='bsql-es'," +
                "'address'='127.0.0.1:9200'," +
                "'indexName'='zzj_test3'," +
                "'typeName'='logs'," +
                "'parallelism'='1'" +
                ")";
        tEnv.executeSql(clickHouseSql);

        String insert = "INSERT INTO es_flink_sink_test SELECT commodity_id,commodity_name,price FROM my_kafka_test";

        tEnv.executeSql(insert);
        tEnv.toRetractStream(table2, Row.class).print("===== ");
        env.execute("");
    }
}
