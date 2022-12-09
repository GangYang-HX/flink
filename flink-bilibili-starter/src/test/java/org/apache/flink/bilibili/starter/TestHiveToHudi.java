package org.apache.flink.bilibili.starter;

import org.apache.flink.bilibili.sql.SqlRegister;
import org.apache.flink.bilibili.sql.SqlTree;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

public class TestHiveToHudi {
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

    public void test() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        StatementSet statementSet = tabEnv.createStatementSet();


    }

    @Test
    public void testHiveToHudiByPartitionName() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
//                + "'type' = 'hive',\n"
//                + "'hive-version' = '2.3.6',\n"
//                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
//                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
//                + "'default-database' = 'yg_test_db'\n"
//                + ")\n";
//        tableEnv.executeSql(createCatalog);

        String createHudi = "create table sink_hudi("
                + "  word varchar,\n"
                + "  num bigint,\n"
                + "  event_type varchar,\n"
                + "  app_id varchar,\n"
                + "  `log_date` varchar(20),\n"
                + "  `log_hour` varchar(20)\n"
                + ") \n"
                + "PARTITIONED BY (log_date,log_hour)\n"
                + "with (\n"
                + "  'connector'='hudi',\n"
                + "  'path'='file:///tmp/test_hive_to_hudi/ods_app_ubt_mall_low_hudi',\n"
                + "  'hoodie.datasource.write.recordkey.field'='event_type,app_id',\n"
                + "  'table.type' = 'MERGE_ON_READ'\n"
                + ")";

        String insertIntoSql =
                "insert into sink_hudi SELECT word,num,event_type,app_id,log_date,log_hour from HIVE_CATALOG_TEST.yg_test_db.test_part_table_05 "
                        + "/*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.consume-start-offset'='log_date=20221009/log_hour=17'"
                        + ")*/";

        StringBuilder sbu = new StringBuilder();
        sbu.append(createHudi);
        sbu.append(";");
        sbu.append(insertIntoSql);
//
//
//        tableEnv.executeSql(createHudi);
//        String insertIntoSql =
//                "insert into sink_hudi SELECT word,num,event_type,app_id,log_date,log_hour from HIVE_CATALOG.yg_test_db.test_part_table_05 "
//                        + "/*+ OPTIONS("
//                        + "'streaming-source.enable'='true'"
//                        + ",'streaming-source.monitor-interval' = '10s'"
//                        + ",'streaming-source.partition-order' = 'partition-name'"
//                        + ",'streaming-source.consume-start-offset'='log_date=20221009/log_hour=17'"
//                        + ")*/";
//        tableEnv.executeSql(insertIntoSql);

//        tableEnv.executeSql("CREATE TABLE sink_print_log (\n"
//                + "  word varchar,\n"
//                + "  num bigint,\n"
//                + "  event_type varchar,\n"
//                + "  app_id varchar,\n"
//                + "  log_date varchar,\n"
//                + "  log_hour varchar\n"
//                + ") WITH ('connector' = 'print')");
//
//        String selectSql =
//                "SELECT word,num,event_type,app_id,log_date,log_hour from HIVE_CATALOG.yg_test_db.test_part_table_05 "
//                        + "/*+ OPTIONS("
//                        + "'streaming-source.enable'='true'"
//                        + ",'streaming-source.monitor-interval' = '10s'"
//                        + ",'streaming-source.partition-order' = 'partition-name'"
//                        + ",'streaming-source.consume-start-offset'='log_date=20221009/log_hour=17'"
//                        + ")*/";

//        tableEnv.createTemporaryView("temp_hive_table", tableEnv.sqlQuery(selectSql));
//
//        tableEnv
//                .executeSql(
//                        "insert into sink_print_log SELECT word,num,event_type,app_id,log_date,log_hour from temp_hive_table");
        StatementSet statementSet = tableEnv.createStatementSet();
        SqlRegister register = new SqlRegister(env, tableEnv, statementSet);

        SqlTree sqlTree = new SqlTree(sbu.toString());
        register.register(sqlTree, false);

        statementSet.execute("test_hive_to_hudi");
    }
}
