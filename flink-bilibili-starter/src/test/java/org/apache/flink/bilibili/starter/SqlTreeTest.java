package org.apache.flink.bilibili.starter;

import org.apache.flink.bilibili.sql.SqlRegister;
import org.apache.flink.bilibili.sql.SqlTree;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zhangyang
 * @Date:2022/6/6
 * @Time:01:11
 */
public class SqlTreeTest {

    @Test
    public void test01() throws IOException {

        initKerberos();
        String multiSql = "CREATE CATALOG hive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'hive-conf-dir' = '/Users/galaxy/Desktop/uat-hdfs',\n" +
                "    'hadoop-conf-dir' = '/Users/galaxy/Desktop/uat-hdfs',\n" +
                "    'default-database' = 'bili_bdp',\n" +
                "    'hive-mv-path' = 'hdfs://bili-test-ns1/tmp/zhangyang'\n" +
                ");\n" +
                "\n" +
                "create materialized view hive.bili_bdp.wechat_live_flow_features_mv_all_pvuv_inner_group with ('primary_key' = 'log_date,buvid') as\n" +
                "  select\n" +
                "      log_date,\n" +
                "      buvid,\n" +
                "      max(log_hour) as log_hour,\n" +
                "      max(log_minute) as log_minute,\n" +
                "      count(1) as pv,\n" +
                "      1 as uv\n" +
                "    from\n" +
                "      hive.bili_bdp.wechat_live_flow_features_dwd\n" +
                "    group by\n" +
                "      log_date,\n" +
                "      buvid;";
        SqlTree tree = new SqlTree(multiSql);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        StatementSet statementSet = tabEnv.createStatementSet();

        SqlRegister register = new SqlRegister(env, tabEnv, statementSet);
        register.register(tree, true);
    }

    private static void initKerberos() throws IOException {

        String env = "/Users/galaxy/Desktop/uat-hdfs";
        System.setProperty("java.security.krb5.conf", env + "/krb5.conf");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(env, "core-site.xml"));
        conf.addResource(new Path(env, "hdfs-site.xml"));
        conf.addResource(new Path(env, "mount-table.xml"));

        String principal = "relayadmin/admin@BILIBILI.CO";
        String keytab = env + "/relayadmin.keytab";

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
}
