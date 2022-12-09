package com.bilibili.bsql.hdfs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.Objects;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHdfsTest.java
 * @description This is the description of BsqlHdfsTest.java
 * @createTime 2020-10-23 12:05:00
 */
public class BsqlHdfsTest {

    @Test
    public void hdfsTest() throws Exception {
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

        String sql = "CREATE TABLE datagen_source (`col1` int, `product` BIGINT, `amount` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1','fields.col1.min' = '1','fields.col1.max' = '10')";
        tEnv.executeSql(sql);

        tEnv.executeSql("CREATE TABLE hdfs_side_table (\n" +
                "c1 varchar,\n" +
                "c2 varchar,\n" +

                "PRIMARY KEY (c1) NOT ENFORCED\n" +
                ") WITH(\n" +
                "'path' = '/Users/jinzhengyu/Desktop/lzo',\n" +
                "'period' = 'day:1',\n" +
                "'parallelism' = '40',\n" +
                "'bsql-delimit.delimiterKey' = '\u0001',\n" +
                "'connector' = 'bsql-hdfs'\n" +
                ")");

        // join the async table
        Table result = tEnv.sqlQuery("select cast(a.col1 as string),b.c2 from datagen_source as a left join hdfs_side_table FOR SYSTEM_TIME AS OF now() AS b on cast(a.col1 as string) = b.c1");

        tEnv.toRetractStream(result, RowData.class).print("===== ");
        env.execute("");
    }

    @Test
    public void hdfsSourceTest() throws Exception {
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

        String sql = "CREATE TABLE wxm_test " +
                "(`userss` VARCHAR," +
                "`aa` BIGINT," +
                "`bb` VARCHAR," +
                "`cc` VARCHAR," +
                "`dd` VARCHAR," +
                "`ee` VARCHAR," +
                "`hh` VARCHAR," +
                "`ii` VARCHAR," +
                "`gg` VARCHAR," +
                "`kk` VARCHAR," +
                "`ff` VARCHAR" +
                ") " +
                "WITH (" +
                "'connector'='bsql-hdfs-source'," +
                "'conf'='/Users/weiximing/software/hadoop-2.8.3/etc/hadoop'," +
                "'path'='/user/wxm'," +
                "'user'='hdfs'," +
                "'bsql-delimit.delimiterKey' = '**',\n" +
                "'parallelism'='1'" +
                ")";
        tEnv.executeSql(sql);

        Table result = tEnv.sqlQuery("select * from wxm_test");

        tEnv.toRetractStream(result, RowData.class).print("===== ");
        env.execute("");
    }
}
