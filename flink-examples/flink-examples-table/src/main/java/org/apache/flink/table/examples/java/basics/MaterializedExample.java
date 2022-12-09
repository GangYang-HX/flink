package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.io.IOException;

/**
 * Example for creating materialized view with the table & SQL api. The example shows how to create,
 * query of group by a table.
 */
public class MaterializedExample {
    public static void main(String[] args) throws IOException {

        KerberosUtil.init();

        Configuration conf = new Configuration();
        conf.set(OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_ENABLED, true);
        conf.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_DB_URL,
                "jdbc:mysql://172.23.47.37:5594/hudi_manager?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false");
        conf.set(OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_USER, "hudi_manager");
        conf.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_PWD,
                "EQTdMJHNk8kJfjtryF129j7w3v8BgO79");
        conf.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_BOOTSTRAP,
                "jssz-lowlevel-pakafka-01.host.bilibili.co:9092,jssz-lowlevel-pakafka-02.host.bilibili.co:9092,jssz-lowlevel-pakafka-03.host.bilibili.co:9092,jssz-lowlevel-pakafka-04.host.bilibili.co:9092,jssz-lowlevel-pakafka-05.host.bilibili.co:9092");
        conf.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_TOPIC,
                "r_ods.flink_batch_mv_sub_query");
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

        //        String createMv = "create materialized view
        // bili_bdp.rtd_live_highlevel_watch_guid_hudi_mv_01 as SELECT CAST(`window_start` AS
        // STRING) AS `window_start`, CAST(`window_end` AS STRING) AS `window_end`, `appid`,
        // `area_id`, COUNT(DISTINCT `buvid`) AS `uv`" +
        //                " FROM TABLE(HOP(TABLE `rtd_live_highlevel_watch_guid_hudi`,
        // DESCRIPTOR(`time_iso`), INTERVAL '5' MINUTE, INTERVAL '60' MINUTE)) " +
        //                "GROUP BY `window_start`, `window_end`, `appid`, `area_id`";
        //        env.executeSql(createMv);

        String querySql =
                "explain\n"
                        + "select\n"
                        + "  /*+ OPTIONS('table.optimizer.materialization-enabled'='true') */\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  appid,\n"
                        + "  area_id,\n"
                        + "  cnt\n"
                        + "from\n"
                        + "  (\n"
                        + "    select\n"
                        + "      cast(window_start as varchar) as window_start,\n"
                        + "      cast(window_end as varchar) as window_end,\n"
                        + "      appid,\n"
                        + "      area_id,\n"
                        + "      count(distinct buvid) as cnt\n"
                        + "    from\n"
                        + "      TABLE(\n"
                        + "        HOP(\n"
                        + "          TABLE bili_bdp.rtd_live_highlevel_watch_guid_hudi,\n"
                        + "          DESCRIPTOR(time_iso),\n"
                        + "          INTERVAL '5' MINUTES,\n"
                        + "          INTERVAL '60' MINUTES\n"
                        + "        )\n"
                        + "      )\n"
                        + "    group by\n"
                        + "      window_start,\n"
                        + "      window_end,\n"
                        + "      appid,\n"
                        + "      area_id\n"
                        + "  ) a\n"
                        + "where\n"
                        + "  window_end = '2022-08-15 12:50:00.000'\n"
                        + "  and appid = 1\n"
                        + "  and area_id in (13,14)\n"
                        + "";
        env.executeSql(querySql).print();
    }
}
