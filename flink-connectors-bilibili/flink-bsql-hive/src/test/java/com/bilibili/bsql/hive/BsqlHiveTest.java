package com.bilibili.bsql.hive;//package com.bilibili.bsql.hive;
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.contrib.streaming.state.PredefinedOptions;
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
//import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
//import org.apache.flink.runtime.state.StateBackend;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.runtime.util.HadoopUtils;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.data.RowData;
//import org.apache.hadoop.conf.Configuration;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.Objects;
//
///**
// * Created with IntelliJ IDEA.
// *
// * @author weiximing
// * @version 1.0.0
// * @className BsqlHiveTest.java
// * @description This is the description of BsqlHiveTest.java
// * @createTime 2020-11-05 18:14:00
// */
//public class BsqlHiveTest {
//
//    @Test
//    public void hiveSinkTest() throws Exception {
//        String[] args = new String[0];
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        String planner = params.has("planner") ? params.get("planner") : "blink";
//
//        // set up execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE);
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        //  RocksDBStateBackend 只支持异步快照，支持增量快照
//        String bathPath = "file:///Users/weiximing/code/gitlab/flinkdemo/";
//        String path = bathPath + "checkpoint";
//        String rocksPath = bathPath + "rocksdb";
//        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(path);
//        rocksDBStateBackend.setDbStoragePath(rocksPath);
//        rocksDBStateBackend.setNumberOfTransferingThreads(1);
//        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
//        env.setStateBackend((StateBackend) new FsStateBackend(path, true));
//
//        StreamTableEnvironment tEnv;
//        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
//            EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                    .inStreamingMode()
//                    .useBlinkPlanner()
//                    .build();
//            tEnv = StreamTableEnvironment.create(env, settings);
//        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
//            EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                    .inStreamingMode()
//                    .useOldPlanner()
//                    .build();
//            tEnv = StreamTableEnvironment.create(env, settings);
//        } else {
//            System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
//                    "where planner (it is either flink or blink, and the default is blink) indicates whether the " +
//                    "example uses flink planner or blink planner.");
//            return;
//        }
//
//        String sql = "CREATE TABLE wxm_test (`userss` VARCHAR, `product` BIGINT, `amount` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
//        tEnv.executeSql(sql);
//
//        Table table2 = tEnv.sqlQuery("select * from wxm_test");
//
//        String clickHouseSql = "CREATE TABLE hive_flink_sink_test (info_index VARCHAR,courier_id BIGINT,city_id BIGINT" +
//                ") WITH (" +
//                "'connector.type'='bsql-hive'," +
//                "'tableName'='ods.ods_app_ubt'," +
//                // "'location'='/Users/weiximing/code/gitlab/saber/saber-connectors/saber-connector-hive/src/test/java/com/bilibili/bsql/hive'," +
//                "'location'='viewfs://uat-bigdata-cluster/flume/flink_test'," +
//                "'parallelism'='1'" +
//                ")";
//        tEnv.executeSql(clickHouseSql);
//
//        Table clickHouseTable = tEnv.sqlQuery("select `userss` as info_index,`product` as courier_id,`amount` as city_id from " + table2);
//
//        tEnv.executeSql("INSERT INTO hive_flink_sink_test SELECT info_index,courier_id,city_id FROM " + clickHouseTable);
//        tEnv.toRetractStream(table2, RowData.class).print("===== ");
//        env.execute("");
//    }
//
//    @Test
//    public void testHdfs() throws Exception {
//        /*Path path = new Path("viewfs://uat-bigdata-cluster/warehouse1/");
//        //FileSystem.get(path.toUri());
//        Configuration configuration = new Configuration();
//        configuration.addResource(new org.apache.hadoop.fs.Path("/Users/weiximing/software/hadoop-2.8.3/etc/hadoop/mount-table.xml"));
//        configuration.addResource(new org.apache.hadoop.fs.Path("/Users/weiximing/software/hadoop-2.8.3/etc/hadoop/hdfs-site.xml"));
//        configuration.addResource(new org.apache.hadoop.fs.Path("/Users/weiximing/software/hadoop-2.8.3/etc/hadoop/core-site.xml"));
//        String schema = org.apache.hadoop.fs.FileSystem.get(path.toUri(), configuration).getScheme();
//
//        Configuration config = HadoopUtils.getHadoopConfiguration(new org.apache.flink.configuration.Configuration());
//        System.out.println(config);
//*/
//        String ttl = verificationPath("viewfs://uat-bigdata-cluster/warehouse1");
//
//        System.out.println(ttl);
//
//        //System.out.println(schema);
//
//    }
//
//    private String verificationPath(String location) {
//        try {
//            HadoopFileSystem fileSystem = (HadoopFileSystem) FileSystem.get(new org.apache.hadoop.fs.Path(location).toUri());
//            location = fileSystem.getHadoopFileSystem().resolvePath(new org.apache.hadoop.fs.Path(location)).toString();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return location;
//    }
//}
