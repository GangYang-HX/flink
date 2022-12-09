package com.bilibili.bsql.redis;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.util.Objects;

/**
 * @author zhuzhengjun
 * @date 2020/11/12 10:05 下午
 */
public class HbaseSideTest {
    @Test
    public void getHbaseSide() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        String sql = "CREATE TABLE hbase_test (`userss` STRING, `rowkey` Int, `amount` BIGINT,`amount01` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
        tEnv.executeSql(sql);

        Table table2 = tEnv.sqlQuery("select * from hbase_test");

        tEnv.executeSql("CREATE TABLE HbaseSide (\n" +
                "  -- declare the schema of the table\n" +
                "  `rowkey` INT,\n" +
                "  `cf` ROW<data STRING>,\n" +
//                "  `family2`ROW<q2 STRING, q3 BIGINT>,\n" +
//                "  `family3` ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,\n" +
                "   PRIMARY KEY(rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-hbase',\n" +
                "  'tableName' = 'bossview_no_ratelimiter',\n" +
                "  'zookeeperQuorum' = '10.69.89.30:2181,10.69.136.33:2181,10.69.178.15:2181',\n" +
//                "  'delimiterKey' = '|',\n" +
                "  'parallelism' = '5',\n" +
                "  'zookeeperParent' = '/hbase'\n" +
                ")");

        Table result = tEnv.sqlQuery("SELECT * FROM hbase_test as a" +
                " left join " +
                "HbaseSide FOR SYSTEM_TIME AS OF now() AS r " +
                "on a.rowkey=r.rowkey");

        tEnv.toRetractStream(result, RowData.class).print("===== ");
        env.execute("");
    }
}
