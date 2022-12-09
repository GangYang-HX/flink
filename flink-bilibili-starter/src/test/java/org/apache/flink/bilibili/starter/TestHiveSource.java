package org.apache.flink.bilibili.starter;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TestHiveSource {


    @Before
    public void initKerberos() throws Exception {
        String env = "/Users/yanggang/Downloads/conf";
        System.setProperty("java.security.krb5.conf", env + "/keytabs/krb5.conf");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(env, "core-site.xml"));
        conf.addResource(new Path(env, "hdfs-site.xml"));
        conf.addResource(new Path(env, "hive-site.xml"));
        conf.addResource(new Path(env, "yarn-site.xml"));

        String principal = "admin/admin@BILIBILI.CO";
        String keytab = env + "/keytabs/admin.keytab";

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation loginUser;
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } else {
            return;
        }
        loginUser = UserGroupInformation.getCurrentUser();
        System.out.println(loginUser);
    }

    @Test
    public void testPrintHiveSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        String createCatalog = "CREATE CATALOG my_hive_catalog WITH (\n"
//                + "'type' = 'hive',\n"
//                + "'hive-version' = '2.3.6',\n"
//                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
//                + "'default-database' = 'b_ods'\n"
//                + ")\n";
//        tableEnv.executeSql(createCatalog);
//        String selectSql = "SELECT test,test1 from my_hive_catalog.b_ods.dwd_s_sjptb_kafka_to_keeper_web_task_teest_l_d_ab /*+ OPTIONS('streaming-source.enable'='true')*/";


//        String createTable = "CREATE TABLE hive_source(\n"
//                + "  test VARCHAR,\n"
//                + "  test1 VARCHAR\n"
//                + ") WITH (\n"
//                + "  'connector' = 'hive',\n"
//                + "  'tableName' = 'b_ods.dwd_s_sjptb_kafka_to_keeper_web_task_teest_l_d_ab',\n"
//                + "  'hive-conf-dir'='/Users/yanggang/Downloads/conf',\n"
//                + "  'streaming-source.partition-order' = 'create-time',\n"
//                + "  'streaming-source.enable'='true'\n"
//                + ");\n";

        String createTable = "CREATE TABLE hive_source(\n"
                + "  word VARCHAR,\n"
                + "  num bigint\n"
                + ") WITH (\n"
                + "  'connector' = 'hive',\n"
                + "  'tableName' = 'yg_test_db.test_part_table3',\n"
                + "  'partitionKey' = 'dt',\n"
                + "  'hive-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "  'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "  'streaming-source.partition-order' = 'partition-name',\n"
                + "  'streaming-source.consume-start-offset' = '2022-09-19',\n"
                + "  'streaming-source.enable'='true'\n"
                + ");\n";

        tableEnv.executeSql(createTable);
        tableEnv.executeSql("SELECT word,num from hive_source").print();

        env.execute();
    }

    @Test
    public void testPrintHiveSource2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);

//        String selectSql = "SELECT word,num from HIVE_CATALOG.yg_test_db.test_part_table3 "
//                + "/*+ OPTIONS('streaming-source.enable'='true'"
//                + ",'streaming-source.monitor-interval' = '3s'"
//                + ",'streaming-source.partition-order' = 'create-time'"
//                + ",'streaming-source.consume-start-offset'='2022-09-10 12:10:00')*/";

        String selectSql = "SELECT word,num from HIVE_CATALOG.yg_test_db.test_part_table3 "
                + "/*+ OPTIONS('streaming-source.enable'='true'"
                + ",'streaming-source.monitor-interval' = '3s'"
                + ",'streaming-source.partition-order' = 'partition-name'"
                + ",'streaming-source.consume-start-offset'='dt=20220918')*/";
        tableEnv.executeSql(selectSql).print();

        env.execute();
    }

    @Test
    public void testTimeZone() {
        Timestamp timestamp = new Timestamp(1663817592000L);

        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        System.out.println(localDateTime.toLocalDate().toEpochDay() * 86400000L);

        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        LocalDateTime shLocal = LocalDateTime.now(zoneId);
        System.out.println(
                shLocal.minusSeconds(1663817592000L).toLocalDate().toEpochDay() * 86400000L);
    }


    @Test
    public void testFourLevelHivePartionByPartitionName() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);


        String selectSql =
                "SELECT word,num,event_type,app_id from HIVE_CATALOG.yg_test_db.test_four_part_table "
                        + "/*+ OPTIONS('streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.consume-start-offset'='log_date=20220919/log_hour=13')*/";

        System.out.println("start consumer hive source =====>>>" + new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss").format(new Date()));

//        tableEnv.executeSql(selectSql).print();
        tableEnv.toDataStream(tableEnv.sqlQuery(selectSql)).map(new MapFunction<Row, String>() {
            private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public String map(Row value) throws Exception {
                return "获取时间==》》" + df.format(new Date()) + "===>>>>" + value.toString();
            }
        }).print();

        env.execute();
    }

    @Test
    public void testFourLevelHivePartionByPartitionTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);

        String selectSql = "SELECT word,num from HIVE_CATALOG.yg_test_db.test_four_part_table "
                + "/*+ OPTIONS('streaming-source.enable'='true'"
//                + ",'streaming-source.monitor-interval' = '3s'"
                + ",'streaming-source.partition-order' = 'partition-time'"
                + ",'partition.time-extractor.timestamp-pattern' = '$log_date $log_hour:00:00'"
                + ",'partition.time-extractor.timestamp-formatter' = 'yyyyMMdd HH:mm:ss'"
                + ",'streaming-source.consume-start-offset'='20220918 12:10:00')*/";

        //关于key的填写前后不能有空格
        //这个填写格式必须和timestamp-formatter保持一致，timestamp-formatter必须要和hive表的分区字段定义保持一致

        System.out.println("start consumer hive source =====>>>" + new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss").format(new Date()));

//        tableEnv.executeSql(selectSql).print();
        tableEnv.toDataStream(tableEnv.sqlQuery(selectSql)).map(new MapFunction<Row, String>() {
            private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public String map(Row value) throws Exception {
                return "获取时间==》》" + df.format(new Date()) + "===>>>>" + value.toString();
            }
        }).print();

        env.execute();
    }

    @Test
    public void testFourLevelHivePartionByCreateTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

//        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);
//
//        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);


//        String selectSql = "SELECT word,num from HIVE_CATALOG.yg_test_db.test_four_part_table "
//                + "/*+ OPTIONS("
//                + "'streaming-source.enable'='true'"
//                + ",'streaming-source.monitor-interval' = '10s'"
//                + ",'streaming-source.partition-order' = 'create-time'"
//                + ",'streaming-source.partition.include' = 'all'"
//                + ",'table.exec.hive.load-partition-splits.thread-num' = '10'"
//                + ",'streaming-source.consume-start-offset'='2022-09-19 17:00:00'"
//                + ")*/";

        String selectSql = "SELECT word,num from HIVE_CATALOG.yg_test_db.test_part_table_05 "
                + "/*+ OPTIONS("
                + "'streaming-source.enable'='true'"
                + ",'streaming-source.monitor-interval' = '10s'"
                + ",'streaming-source.partition-order' = 'create-time'"
                + ",'streaming-source.partition.include' = 'all'"
                + ",'table.exec.hive.load-partition-splits.thread-num' = '10'"
                + ",'streaming-source.consume-start-offset'='2022-09-19 17:00:00'"
                + ")*/";

        System.out.println("start consumer hive source =====>>>" + new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss").format(new Date()));

//        tableEnv.executeSql(selectSql).print();
        tableEnv.toDataStream(tableEnv.sqlQuery(selectSql)).map(new MapFunction<Row, String>() {
            private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public String map(Row value) throws Exception {
                return "获取时间==》》" + df.format(new Date()) + "===>>>>" + value.toString();
            }
        }).print();

        env.execute();
    }

    @Test
    public void testTwoLevelHivePartitionByCreateTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);

        String selectSql =
                "SELECT word,num,event_type,app_id from HIVE_CATALOG.yg_test_db.test_part_table_05 "
                        + "/*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'create-time'"
                        + ",'streaming-source.partition.include' = 'all'"
                        + ",'table.exec.hive.load-partition-splits.thread-num' = '10'"
                        + ",'streaming-source.consume-start-offset'='2022-10-10 11:00:00'"
                        + ")*/";

        System.out.println("start consumer hive source =====>>>" + new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss").format(new Date()));

//        tableEnv.executeSql(selectSql).print();
        tableEnv.toDataStream(tableEnv.sqlQuery(selectSql)).map(new MapFunction<Row, String>() {
            private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public String map(Row value) throws Exception {
                return "获取时间==》》" + df.format(new Date()) + "===>>>>" + value.toString();
            }
        }).print();

        env.execute();
    }

    @Test
    public void testTwoLevelHivePartitionByPartitionName() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);

        String selectSql =
                "SELECT word,num,event_type,app_id,log_date,log_hour from HIVE_CATALOG.yg_test_db.test_part_table_05 "
                        + "/*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
//                        + ",'streaming-source.partition.include' = 'all'"
//                        + ",'table.exec.hive.load-partition-splits.thread-num' = '10'"
                        + ",'streaming-source.consume-start-offset'='log_date=20221013/log_hour=14'"
                        + ")*/";

        System.out.println("start consumer hive source =====>>>" + new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss").format(new Date()));

//        tableEnv.executeSql(selectSql).print();
        tableEnv.toDataStream(tableEnv.sqlQuery(selectSql)).map(new MapFunction<Row, String>() {
            private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public String map(Row value) throws Exception {
                return "获取时间==》》" + df.format(new Date()) + "===>>>>" + value.toString();
            }
        }).print();

        env.execute();
    }


    @Test
    public void testHiveSourceTemporalJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
//                + "'type' = 'hive',\n"
//                + "'hive-version' = '2.3.6',\n"
//                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
//                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
//                + "'default-database' = 'yg_test_db'\n"
//                + ")\n";
//        tableEnv.executeSql(createCatalog);

        DataStream<Tuple2<String, String>> source = env.addSource(new SourceFunction<Tuple2<String, String>>() {
            private List<String> words = Arrays.asList(
                    "woshinidaye",
                    "fuck",
                    "cn",
                    "un",
                    "fk",
                    "pl");
            private List<String> likes = Arrays.asList(
                    "dalanqiu",
                    "dapingbangqiu",
                    "tizuqiu",
                    "youyong",
                    "fangniu",
                    "fangyazi");

            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                while (true) {
                    ctx.collect(Tuple2.of(
                            words.get(RandomUtils.nextInt(0, 6)),
                            likes.get(RandomUtils.nextInt(0, 6))));
                    Thread.sleep(200L);
                }
            }

            @Override
            public void cancel() {

            }
        });

        Table sourceTable = tableEnv.fromDataStream(source);
        tableEnv.createTemporaryView("InputTable", sourceTable);

        tableEnv.toDataStream(tableEnv.sqlQuery("select f0,f1 from InputTable")).print();


        env.execute();
    }

}
