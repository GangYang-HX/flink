package org.apache.flink.bilibili.starter;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

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

		URL[] libs = new URL[] {new URL(jar1), new URL(jar2)};
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		Class<?> c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
		Method method = c.getMethod("explain", String.class);

		JSONObject context = new JSONObject();
        //context.put("sql", "CREATE TABLE KafkaSourceTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'source-topic','properties.bootstrap.servers' = 'localhost:9092','properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset','format' = 'csv');CREATE TABLE KafkaSinkTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'sink-topic','properties.bootstrap.servers' = 'localhost:9092','format' = 'csv');insert into KafkaSinkTable select id from KafkaSourceTable;");
        context.put("sql", "CREATE TABLE KafkaSourceTable WITH ('properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset') like Kafka_1.r_ods.dwb_keeper_test;insert into Kafka_1.r_ods.dwb_keeper_test select ai, bi, ci, xx, ddd  from KafkaSourceTable;");


		Object ret = method.invoke(c.newInstance(), context.toJSONString());

		System.out.println(ret);
	}

	@Test
	public void lineAnalysis() throws Exception {

        String jar1 = "file:///Users/luotianran/IdeaProjects/flink-1.15/flink-bilibili-starter/target/flink-bilibili-starter_2.12-1.15.1-SNAPSHOT.jar";
        String jar2 = "file:///Users/luotianran/IdeaProjects/flink-1.15/flink-dist/target/flink-dist_2.12-1.15.1-SNAPSHOT.jar";

		URL[] libs = new URL[] {new URL(jar1), new URL(jar2) };
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		Class c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
		Method method = c.getMethod("lineAnalysis", String.class);

		JSONObject context = new JSONObject();
        //context.put("sql", "CREATE TABLE KafkaSourceTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'source-topic','properties.bootstrap.servers' = 'localhost:9092','properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset','format' = 'csv');CREATE TABLE KafkaSinkTable (`id` STRING) WITH ('connector' = 'kafka','topic' = 'sink-topic','properties.bootstrap.servers' = 'localhost:9092','format' = 'csv');insert into KafkaSinkTable select id from KafkaSourceTable;");
        context.put("sql", "CREATE TABLE KafkaSourceTable WITH ('properties.group.id' = 'testGroup','scan.startup.mode' = 'earliest-offset') like Kafka_1.r_ods.dwb_keeper_test;insert into Kafka_1.r_ods.dwb_keeper_test select ai, bi, ci, xx, ddd  from KafkaSourceTable;");

        Object ret = method.invoke(c.newInstance(), context.toJSONString());

		System.out.println(ret);
	}
}
