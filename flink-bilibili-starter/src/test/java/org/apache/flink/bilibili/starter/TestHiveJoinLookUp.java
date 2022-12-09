package org.apache.flink.bilibili.starter;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

public class TestHiveJoinLookUp {

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
    public void testJoinByPartitionName() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(5);

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
                "SELECT t1.word,dim.event_type from HIVE_CATALOG.yg_test_db.test_part_table_05 /*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.consume-start-offset'='log_date=20221021/log_hour=15'"
                        + ")*/ as t1 left join \n"
                        + "HIVE_CATALOG.yg_test_db.test_part_table_08 /*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '1min'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.partition.include' = 'latest'"
                        + ")*/ FOR SYSTEM_TIME AS OF now() AS dim ON t1.word = dim.word";

        tableEnv.executeSql(selectSql).print();
        env.execute();
    }

    @Test
    public void testJoinByPartitionName2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

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

        String createView =
                "create view join1 as SELECT t1.word,dim.event_type from HIVE_CATALOG.yg_test_db.test_part_table_05 /*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.consume-start-offset'='log_date=20221021/log_hour=15'"
                        + ")*/ as t1 left join \n"
                        + "HIVE_CATALOG.yg_test_db.test_part_table_09 /*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '1min'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.partition.include' = 'latest'"
                        + ")*/ FOR SYSTEM_TIME AS OF now() AS dim ON t1.word = dim.word";

        tableEnv.executeSql(createView);


        String selectSql =
                "SELECT t1.word,dim.event_type from join1 as t1 left join \n"
                        + "HIVE_CATALOG.yg_test_db.test_part_table_08 /*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '1min'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.partition.include' = 'latest'"
                        + ")*/ FOR SYSTEM_TIME AS OF now() AS dim ON t1.word = dim.word";
        tableEnv.executeSql(selectSql).print();

//        tableEnv.executeSql("select word,event_type from join1").print();

        env.execute();
    }

    @Test
    public void testKafkaJoinHiveLookup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createSource = "CREATE TABLE kafka_source_table (\n"
                + "   word string,\n"
                + "   num bigint,\n"
                + "   event_type string,\n"
                + "   app_id string, \n"
                + "   log_date string,\n"
                + "   log_hour string \n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'ods_s_flink_kafka_source_test3',\n"
                + "  'properties.group.id' = 'testGroup',\n"
                + "  'format' = 'csv',\n"
                + "  'scan.startup.mode' = 'latest-offset',\n"
                + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092')";
        tableEnv.executeSql(createSource);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);

        String selectSql =
                "SELECT t1.word,dim.event_type from kafka_source_table as t1 left join \n"
                        + "HIVE_CATALOG.yg_test_db.test_part_table_05 /*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '1h'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.partition.include' = 'latest')*/ FOR SYSTEM_TIME AS OF now() AS dim ON t1.word = dim.word";

        tableEnv.executeSql(selectSql).print();

//        tableEnv.executeSql("select * from HIVE_CATALOG.yg_test_db.test_part_table_05").print();

        env.execute();


    }

}
