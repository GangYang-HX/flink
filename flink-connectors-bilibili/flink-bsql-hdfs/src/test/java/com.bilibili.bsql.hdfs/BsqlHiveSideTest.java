package com.bilibili.bsql.hdfs;

import com.sun.tools.internal.xjc.Driver;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

/**
 * @Author: JinZhengyu
 * @Date: 2022/7/28 上午10:46
 */
public class BsqlHiveSideTest {

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

        String sql = "CREATE TABLE datagen_source (`col1` int, ts AS localtimestamp,WATERMARK FOR ts AS ts - INTERVAL '8' HOUR) WITH ('connector'='bsql-datagen','rows-per-second'='1','fields.col1.min' = '1','fields.col1.max' = '10')";
        tEnv.executeSql(sql);

        tEnv.executeSql("CREATE TABLE side_table (\n" +
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

        tEnv.executeSql(
                "CREATE TABLE print_sink (\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print',\n"
                        + "  'print-identifier' = 'printa'\n"
                        + ")");

        // join the async table
//        Table result = tEnv.sqlQuery("select cast(a.col1 as string),b.c2,a.ts from datagen_source as a left join side_table FOR SYSTEM_TIME AS OF now() AS b on cast(a.col1 as string) = b.c1");
        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("insert into print_sink select cast(a.col1 as string),b.c2,cast(a.ts as string) from datagen_source as a left join side_table FOR SYSTEM_TIME AS OF now() AS b on cast(a.col1 as string) = b.c1");

        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }

    }


    @Test
    public void hiveSideTest() throws Exception {
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

        String sql = "CREATE TABLE datagen_source (`col1` int, ts AS localtimestamp,WATERMARK FOR ts AS ts - INTERVAL '8' HOUR) WITH ('connector'='bsql-datagen','rows-per-second'='1','fields.col1.min' = '1','fields.col1.max' = '10')";
        tEnv.executeSql(sql);

        tEnv.executeSql("CREATE TABLE side_table (\n" +
                "c1 varchar,\n" +
                "c2 varchar\n" +
                ") WITH(\n" +
//                "'tableName' = 'bili_dim.dim_test_xinxue',\n" +
//                "'tableName' = 'b_dwm.live_dwd_prty_memb_rel_spm_record_l_d',\n" +
                "'tableName' = 'b_r_dwd.ods_hive_ddl_event_sink_table',\n" +
//                "'tableName' = 'bili_dim.xinxue_test0804',\n" +
//                "'tableName' = 'bili_dim.dim_cate_word_package_ctwh_v4',\n" +
                "'joinKey' = 'c1',\n" +
                "'useMem' = 'true',\n" +
                "'checkSuccess' = 'true',\n" +
                "'connector' = 'bsql-hive-side'\n" +
                ")");

        tEnv.executeSql(
                "CREATE TABLE print_sink (\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 STRING,\n"
                        + "  col4 STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print',\n"
                        + "  'print-identifier' = 'printa'\n"
                        + ")");
        StatementSet stateSet = tEnv.createStatementSet();

        stateSet.addInsertSql("insert into print_sink select cast(a.col1 as string),b.c1,b.c2,cast(a.ts as string) from datagen_source as a left join side_table FOR SYSTEM_TIME AS OF now() AS b on cast(a.col1 as string) = b.c1");

        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }


    @Test
    public void hiveSideMulTest() throws Exception {
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

        String sql = "CREATE TABLE datagen_source (`col1` int, ts AS localtimestamp,WATERMARK FOR ts AS ts - INTERVAL '8' HOUR) WITH ('connector'='bsql-datagen','rows-per-second'='1','fields.col1.min' = '1','fields.col1.max' = '10')";
        tEnv.executeSql(sql);

        tEnv.executeSql("CREATE TABLE side_table1 (\n" +
                "c1 varchar,\n" +
                "c2 varchar\n" +
                ") WITH(\n" +
//                "'tableName' = 'bili_dim.dim_test_xinxue',\n" +
                "'tableName' = 'b_dwm.live_dwd_prty_memb_rel_spm_record_l_d',\n" +
//				"'tableName' = 'b_r_dwd.ods_hive_ddl_event_sink_table',\n" +
//                "'tableName' = 'bili_dim.xinxue_test0804',\n" +
//                "'tableName' = 'bili_dim.dim_cate_word_package_ctwh_v4',\n" +
                "'joinKey' = 'c1',\n" +
                "'useMem' = 'true',\n" +
                "'checkSuccess' = 'true',\n" +
                "'connector' = 'bsql-hive-side'\n" +
                ")");


        tEnv.executeSql("CREATE TABLE side_table2 (\n" +
                "c1 varchar,\n" +
                "c2 varchar\n" +
                ") WITH(\n" +
//                "'tableName' = 'bili_dim.dim_test_xinxue',\n" +
//                "'tableName' = 'b_dwm.live_dwd_prty_memb_rel_spm_record_l_d',\n" +
                "'tableName' = 'b_r_dwd.ods_hive_ddl_event_sink_table',\n" +
//                "'tableName' = 'bili_dim.xinxue_test0804',\n" +
//                "'tableName' = 'bili_dim.dim_cate_word_package_ctwh_v4',\n" +
                "'joinKey' = 'c1',\n" +
                "'useMem' = 'true',\n" +
                "'checkSuccess' = 'true',\n" +
                "'connector' = 'bsql-hive-side'\n" +
                ")");

        tEnv.executeSql(
                "CREATE TABLE print_sink (\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 STRING,\n"
                        + "  col4 STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print',\n"
                        + "  'print-identifier' = 'printa'\n"
                        + ")");
        StatementSet stateSet = tEnv.createStatementSet();

        stateSet.addInsertSql("insert into print_sink select cast(a.col1 as string),b1.c2,b2.c2,cast(a.ts as string) from datagen_source as a " +
                "left join side_table1 FOR SYSTEM_TIME AS OF now() AS b1 on cast(a.col1 as string) = b1.c1 " +
                "left join side_table2 FOR SYSTEM_TIME AS OF now() AS b2 on cast(a.col1 as string) = b2.c1");

        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }


    @Test
    public void readORC() throws Exception {
        String[] args = {"data", "file:///Users/jinzhengyu/Desktop/orc/part-0-1050.orc", "file:///Users/jinzhengyu/Desktop/orc/part-1-136"};
        Driver.main(args);
//        Configuration conf = new Configuration();
//        String fileName = "/Users/jinzhengyu/Downloads/part-0-1050.orc";
//        Path path = new Path(fileName);
//        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
//        RecordReader rows = reader.rows();
//        VectorizedRowBatch rowBatchV2 = reader.getSchema().createRowBatchV2();
//        //	  VectorizedRowBatch rowBatch = reader.getSchema().createRowBatch();
//        while (rows.nextBatch(rowBatchV2)) {
//          System.out.println(rowBatchV2.toString());
//        }
    }

    @Test
    public void readOrcFile() throws IOException {
        String fileName = "file:///Users/jinzhengyu/Desktop/orc/part-1-136";
//    String fileName = "file:///Users/jinzhengyu/Desktop/orc/part-0-1050.orc";
        //		String fileName = "part-1-136";
        Path path = new Path(fileName);
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        //		FileSystem fs = path.getFileSystem(conf);
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
        RecordReader rows = reader.rows();
        TypeDescription schema = reader.getSchema();
        VectorizedRowBatch batch = schema.createRowBatch();
        while (rows.nextBatch(batch)) {
            System.out.println(batch.toString());
            for (int r = 0; r < batch.size; r++) {

            }
        }
    }

    @Test
    public void readOrcFile1() throws IOException {
        String fileName = "file:///Users/jinzhengyu/Desktop/orc/part-1-136";
//    String fileName = "file:///Users/jinzhengyu/Desktop/orc/part-0-1050.orc";
        //		String fileName = "part-1-136";
        Path path = new Path(fileName);
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
        EncryptionKey[] columnEncryptionKeys = reader.getColumnEncryptionKeys();

        RecordReader rows = reader.rows();
        TypeDescription schema = reader.getSchema();
        VectorizedRowBatch batch = schema.createRowBatch();
        while (rows.nextBatch(batch)) {
            System.out.println(batch.toString());
            for (int r = 0; r < batch.size; r++) {

            }
        }
    }
}
