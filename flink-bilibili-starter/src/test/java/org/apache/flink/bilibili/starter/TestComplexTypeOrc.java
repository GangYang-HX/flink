package org.apache.flink.bilibili.starter;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

public class TestComplexTypeOrc {
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
                "SELECT key,`value`,`value`.title,log_date,log_hour from HIVE_CATALOG.yg_test_db.bert_v2q2v_kfc "
                        + "/*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
//                        + ",'table.exec.hive.fallback-mapred-reader' = 'true'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.consume-start-offset'='log_date=20221024/log_hour=07'"
                        + ")*/ limit 20";

        tableEnv.executeSql(selectSql).print();
        env.execute();
    }
}
