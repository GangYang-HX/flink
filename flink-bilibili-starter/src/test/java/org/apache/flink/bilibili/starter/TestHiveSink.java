package org.apache.flink.bilibili.starter;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

public class TestHiveSink {

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
    public void testSinkHive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
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
                + "  'properties.group.id' = 'testGroup_01',\n"
                + "  'format' = 'csv',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n" /*earliest-offset,latest-offset*/
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

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(
                "insert into HIVE_CATALOG.yg_test_db.test_part_table_08 "
                        + "/*+ OPTIONS("
                        + "'partition.time-extractor.timestamp-pattern'='$log_date $log_hour:00:00'"
                        + ",'partition.time-extractor.timestamp-formatter' = 'yyyyMMdd HH:mm:ss'"
                        + ",'sink.partition-commit.trigger'='partition-time'"
                        + ",'sink.partition-commit.delay'='10min'"
                        + ",'sink.partition-commit.policy.kind'='metastore,success-file'"
                        + ")*/ "
                        + "select word,num,event_type,app_id,log_date,log_hour from kafka_source_table");
        statementSet.execute();


        tableEnv
                .executeSql(
                        "select word,num,event_type,app_id,log_date,log_hour from kafka_source_table")
                .print();
        env.execute("test_consumer_source");
    }
}
