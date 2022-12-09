package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;

/** FlinkHintExample. */
public class FlinkHintExample {
    public static void main(String[] args) throws IOException {
        KerberosUtil.init();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment env = TableEnvironment.create(settings);

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

        String sql =
                "select /*+ OPTIONS('number-of-rows'='60') */ * from dm_version_record where log_date = '20220722' ";

        env.executeSql(sql).print();
    }
}
