package org.apache.flink.bilibili.starter;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangyang
 * @Date:2021/5/31
 * @Time:8:31 下午
 */
public class ToolBoxStarterTest {

    @Test
    public void explain() throws Exception {

        String jar1 = "file:///Users/luotianran/IdeaProjects/flink-1.15/flink-bilibili-starter/target/flink-bilibili-starter_2.12-1.15.1-SNAPSHOT.jar";
        String jar2 = "file:///Users/luotianran/IdeaProjects/flink-1.15/flink-dist/target/flink-dist_2.12-1.15.1-SNAPSHOT.jar";

        URL[] libs = new URL[]{new URL(jar1), new URL(jar2)};
        ClassLoader loader = new URLClassLoader(
                libs,
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);

        Class<?> c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
        Method method = c.getMethod("explain", String.class);

        JSONObject context = new JSONObject();
        //context.put("sql", "CREATE TABLE KafkaSourceTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'source-topic','properties.bootstrap.servers' = 'localhost:9092','properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset','format' = 'csv');CREATE TABLE KafkaSinkTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'sink-topic','properties.bootstrap.servers' = 'localhost:9092','format' = 'csv');insert into KafkaSinkTable select id from KafkaSourceTable;");
        context.put(
                "sql",
                "CREATE TABLE KafkaSourceTable WITH ('properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset') like Kafka_1.r_ods.dwb_keeper_test;insert into Kafka_1.r_ods.dwb_keeper_test select ai, bi, ci, xx, ddd  from KafkaSourceTable;");


        Object ret = method.invoke(c.newInstance(), context.toJSONString());

        System.out.println(ret);
    }

    @Test
    public void lineAnalysis() throws Exception {

        String jar1 = "file:///Users/luotianran/IdeaProjects/flink-1.15/flink-bilibili-starter/target/flink-bilibili-starter_2.12-1.15.1-SNAPSHOT.jar";
        String jar2 = "file:///Users/luotianran/IdeaProjects/flink-1.15/flink-dist/target/flink-dist_2.12-1.15.1-SNAPSHOT.jar";

        URL[] libs = new URL[]{new URL(jar1), new URL(jar2)};
        ClassLoader loader = new URLClassLoader(
                libs,
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);

        Class c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
        Method method = c.getMethod("lineAnalysis", String.class);

        JSONObject context = new JSONObject();
        //context.put("sql", "CREATE TABLE KafkaSourceTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'source-topic','properties.bootstrap.servers' = 'localhost:9092','properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset','format' = 'csv');CREATE TABLE KafkaSinkTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'sink-topic','properties.bootstrap.servers' = 'localhost:9092','format' = 'csv');insert into KafkaSinkTable select id from KafkaSourceTable;");
        context.put(
                "sql",
                "CREATE TABLE KafkaSourceTable WITH ('properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset') like Kafka_1.r_ods.dwb_keeper_test;insert into Kafka_1.r_ods.dwb_keeper_test select ai, bi, ci, xx, ddd  from KafkaSourceTable;");

        Object ret = method.invoke(c.newInstance(), context.toJSONString());

        System.out.println(ret);
    }

    @Test
    public void testExplain() throws Exception {
        JSONObject context = new JSONObject();
//        String sql = "CREATE CATALOG my_hive_catalog WITH (\n"
//                + "'type' = 'hive',\n"
////                + "'hive-version' = '2.3.6',\n"
////                + "'hive-conf-dir' = '/Users/yanggang/Downloads/hadoop_conf',\n"
//                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
////                + "'hadoop-conf-dir' = '/Users/yanggang/Downloads/hadoop_conf',\n"
//                + "'default-database' = 'b_ods'\n"
//                + ");\n"
//                + "\n"
//                + "CREATE TABLE sink_print_log (\n"
//                + "  `test` VARCHAR,\n"
//                + "  `test1` VARCHAR\n"
//                + ") WITH ('connector' = 'print');\n"
//                + "\n"
//                + "\n"
//                + "insert into sink_print_log SELECT test,test1 from my_hive_catalog.b_ods.dwd_s_sjptb_kafka_to_keeper_web_task_teest_l_d_ab /*+ OPTIONS('streaming-source.enable'='true')*/";

//                String sql = "CREATE CATALOG my_hive_catalog WITH (\n"
//                + "'type' = 'hive',\n"
//                + "'hive-version' = '2.3.6',\n"
//                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
//                + "'hadoop-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
//                + "'default-database' = 'yg_test_db'\n"
//                + ");\n"
//                + "\n"
//                + "CREATE TABLE sink_print_log (\n"
//                + "  `word` VARCHAR,\n"
//                + "  `num` bigint\n"
//                + ") WITH ('connector' = 'print');\n"
//                + "\n"
//                + "\n"
//                + "insert into sink_print_log SELECT word,num from my_hive_catalog.yg_test_db.test_part_table3 /*+ OPTIONS('streaming-source.enable'='true')*/";

        String sql = "CREATE TABLE sink_print_log (\n"
                + "  `word` VARCHAR,\n"
                + "  `num` bigint\n"
                + ") WITH ('connector' = 'print');\n"
                + "\n"
                + "\n"
                + "insert into sink_print_log SELECT word,num from HIVE_CATALOG_TEST.yg_test_db.test_part_table3 /*+ OPTIONS('streaming-source.enable'='true')*/";
        context.put("sql", sql);
        ToolBoxStarter toolBoxStarter = new ToolBoxStarter();
        String ret = toolBoxStarter.explain(context.toJSONString());
        System.out.println("Sql执行计划结果:===>>" + ret);
    }

    @Test
    public void testExplain2() throws Exception {
        JSONObject context = new JSONObject();
        String sql = "CREATE TABLE source_web_task(\n"
                + "  test VARCHAR,\n"
                + "  test1 VARCHAR\n"
                + ") WITH (\n"
                + "  'connector' = 'hive',\n"
                + "  'hive-version' = '2.3.6',\n"
                + "  'tableName' = 'b_ods.dwd_s_sjptb_kafka_to_keeper_web_task_teest_l_d_ab',\n"
                + "  'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "  'streaming-source.partition-order' = 'create-time',\n"
                + "  'streaming-source.enable'='true'\n"
                + ");"
                + "CREATE TABLE sink_print_log (\n"
                + "  `test` VARCHAR,\n"
                + "  `test1` VARCHAR\n"
                + ") WITH ('connector' = 'print');\n"
                + "\n"
                + "\n"
                + "insert into sink_print_log SELECT test,test1 from source_web_task";
        context.put("sql", sql);
        ToolBoxStarter toolBoxStarter = new ToolBoxStarter();
        String ret = toolBoxStarter.explain(context.toJSONString());
        System.out.println("Sql执行计划结果:===>>" + ret);
    }

    @Test
    public void testExplain3() throws Exception {
        JSONObject context = new JSONObject();
        String sql = "CREATE TABLE source_web_task(\n"
                + "  word VARCHAR,\n"
                + "  num bigint\n"
                + ") WITH (\n"
                + "  'connector' = 'hive',\n"
                + "  'hive-version' = '2.3.6',\n"
                + "  'tableName' = 'yg_test_db.test_part_table3',\n"
                + "  'partitionKey' = 'dt',\n"
                + "  'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "  'streaming-source.partition-order' = 'create-time',\n"
                + "  'streaming-source.enable'='true'\n"
                + ");"
                + "CREATE TABLE sink_print_log (\n"
                + "  `word` VARCHAR,\n"
                + "  `num` bigint\n"
                + ") WITH ('connector' = 'print');\n"
                + "\n"
                + "\n"
                + "insert into sink_print_log SELECT word,num from source_web_task";
        context.put("sql", sql);
        ToolBoxStarter toolBoxStarter = new ToolBoxStarter();
        String ret = toolBoxStarter.explain(context.toJSONString());
        System.out.println("Sql执行计划结果:===>>" + ret);
    }

    @Test
    public void testLineAnalysis() throws Exception {
        JSONObject context = new JSONObject();
        String sql = "CREATE TABLE sink_print_log (\n"
                + "  `word` VARCHAR,\n"
                + "  `num` bigint\n"
                + ") WITH ('connector' = 'print');\n"
                + "\n"
                + "\n"
                + "insert into sink_print_log SELECT word,num from HIVE_CATALOG_TEST.yg_test_db.test_part_table3 /*+ OPTIONS('streaming-source.enable'='true')*/";
        context.put("sql", sql);
        context.put("sql", sql);
        ToolBoxStarter toolBoxStarter = new ToolBoxStarter();
        String ret = toolBoxStarter.lineAnalysis(context.toJSONString());
        System.out.println("Sql行检查结果:===>>" + ret);
    }


    @Test
    public void testPhysicalPlan() throws Exception {
        JSONObject context = new JSONObject();
        String sql = "CREATE TABLE sink_print_log (\n"
                + "  `word` VARCHAR,\n"
                + "  `num` bigint\n"
                + ") WITH ('connector' = 'print');\n"
                + "\n"
                + "\n"
                + "insert into sink_print_log SELECT word,num from HIVE_CATALOG_TEST.yg_test_db.test_part_table3 /*+ OPTIONS('streaming-source.enable'='true')*/";
        context.put("sql", sql);  context.put("sql", sql);
        ToolBoxStarter toolBoxStarter = new ToolBoxStarter();
        String ret = toolBoxStarter.physicalPlan(context.toJSONString());
        System.out.println("Sql物理执行图:===>>" + ret);
    }


}
