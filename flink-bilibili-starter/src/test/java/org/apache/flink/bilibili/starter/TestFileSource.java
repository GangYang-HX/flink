package org.apache.flink.bilibili.starter;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

public class TestFileSource { //====>>> org.apache.flink.connector.file.table.FileSystemTableSourceTest

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
    public void printFileData() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  word varchar,\n"
                        + "  num bigint,\n"
                        + "  log_date varchar\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'csv',"
                        + " 'partition.default-name' = 'log_date',"
                        + " 'source.monitor-interval' = '10s',"
                        + " 'path' = '/tmp/TestFileSource'"
                        + ")";

//        String srcTableDdl =
//                "CREATE TABLE MyTable (\n"
//                        + "  word varchar,\n"
//                        + "  num bigint,\n"
//                        + "  log_date varchar,\n"
//                        + "  log_hour varchar,\n"
//                        + "  event_type varchar,\n"
//                        + "  app_id varchar\n"
//                        + ") with (\n"
//                        + " 'connector' = 'filesystem',"
//                        + " 'format' = 'csv',"
//                        + " 'partition.default-name' = 'log_date,log_hour',"
//                        + " 'source.monitor-interval' = '10s',"
//                        + " 'path' = 'hdfs://bili-test-ns1/warehouse1/yg_test_db.db/test_four_part_table'"
//                        + ")";

        tableEnv.executeSql(srcTableDdl);

//        String srcTableWithMetaDdl =
//                "CREATE TABLE MyTableWithMeta (\n"
//                        + "  word varchar,\n"
//                        + "  num bigint,\n"
//                        + "  filemeta STRING METADATA FROM 'file.path'\n"
//                        + ") with (\n"
//                        + " 'connector' = 'filesystem',"
//                        + " 'format' = 'testcsv',"
//                        + " 'path' = '/tmp/TestFileSource')";
//        tableEnv.executeSql(srcTableWithMetaDdl);

        tableEnv.executeSql("select * from MyTable").print();

        env.execute("test_hive_file");
    }
}
