package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Example for creating materialized view with the table & SQL api. The example shows how to create,
 * query of group by a table.
 */
public class MaterializedExample {
    public static void main(String[] args) throws IOException {

        initKerberos();

        Configuration conf = new Configuration();
        conf.set(OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_ENABLED, true);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(conf).inBatchMode().build();
        TableEnvironmentImpl env = (TableEnvironmentImpl) TableEnvironment.create(settings);

        String registerCatalog =
                "CREATE CATALOG hive WITH (\n"
                        + "    'type' = 'hive',\n"
                        + "    'hive-conf-dir' = '/Users/galaxy/Desktop/uat-hdfs',\n"
                        + "    'hadoop-conf-dir' = '/Users/galaxy/Desktop/uat-hdfs',\n"
                        + "    'default-database' = 'bili_bdp',\n"
                        + "    'hive-mv-path' = 'hdfs://bili-test-ns1/tmp/zhangyang'\n"
                        + ")";
        env.executeSql(registerCatalog);
        String switchCatalog = "use catalog hive";
        env.executeSql(switchCatalog);

        String showDatabases = "show databases";
        env.executeSql(showDatabases).print();

        String createMv =
                "create materialized view if not exists bili_bdp.wechat_live_flow_features_mv_all_pvuv as\n"
                        + "select\n"
                        + "  'dtd_all_pvuv' as data_type,\n"
                        + "  log_date,\n"
                        + "  max(log_hour) as log_hour,\n"
                        + "  max(log_minute) as log_minute,\n"
                        + "  '' as area_type,\n"
                        + "  '' as platform,\n"
                        + "  cast(sum(pv) as varchar) as pv,\n"
                        + "  cast(sum(uv) as varchar) as uv\n"
                        + "from\n"
                        + "  (\n"
                        + "    select\n"
                        + "      log_date,\n"
                        + "      buvid,\n"
                        + "      max(log_hour) as log_hour,\n"
                        + "      max(log_minute) as log_minute,\n"
                        + "      count(1) as pv,\n"
                        + "      1 as uv\n"
                        + "    from\n"
                        + "      bili_bdp.wechat_live_flow_features_dwd\n"
                        + "    group by\n"
                        + "      log_date,\n"
                        + "      buvid\n"
                        + "  ) t4\n"
                        + "group by\n"
                        + "  log_date";
        env.executeSql(createMv);

        String createMv2 =
                "create materialized view if not exists bili_bdp.wechat_live_flow_features_mv_all_pvuv_inner_group as\n"
                        + "  select\n"
                        + "      log_date,\n"
                        + "      buvid,\n"
                        + "      max(log_hour) as log_hour,\n"
                        + "      max(log_minute) as log_minute,\n"
                        + "      count(1) as pv,\n"
                        + "      1 as uv\n"
                        + "    from\n"
                        + "      bili_bdp.wechat_live_flow_features_dwd\n"
                        + "    group by\n"
                        + "      log_date,\n"
                        + "      buvid";

        env.executeSql(createMv2);

        String querySql =
                "select\n"
                        + "  'dtd_all_pvuv' as data_type,\n"
                        + "  log_date,\n"
                        + "  cast(sum(pv) as varchar) as pv\n"
                        + "from\n"
                        + "  (\n"
                        + "    select\n"
                        + "      log_date,\n"
                        + "      buvid,\n"
                        + "      max(log_hour) as log_hour,\n"
                        + "      max(log_minute) as log_minute,\n"
                        + "      count(1) as pv,\n"
                        + "      1 as uv\n"
                        + "    from\n"
                        + "      bili_bdp.wechat_live_flow_features_dwd\n"
                        + "    group by\n"
                        + "      log_date,\n"
                        + "      buvid\n"
                        + "  ) t4\n"
                        + "group by\n"
                        + "  log_date";
        String ret = env.explainSql(querySql);
        System.out.println(ret);
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
