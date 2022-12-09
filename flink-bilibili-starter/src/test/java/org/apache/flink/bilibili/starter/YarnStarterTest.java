package org.apache.flink.bilibili.starter;

import org.junit.Test;
import pleiades.component.env.v1.Env;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

public class YarnStarterTest {

	@Test
	public void startTest() throws Exception {

		String jar0 = "file:///Users/galaxy/Desktop/flink1.11_udf_lye.jar";
		String jar1 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-bilibili-starter/target/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
		String jar2 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-dist/target/flink-dist_2.11-1.11.4-SNAPSHOT.jar";

		URL[] libs = new URL[]{new URL(jar0), new URL(jar1), new URL(jar2)};
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		YarnStarter.main(new String[]{
			"/Users/haozhugogo/IdeaProjects/flink/flink-bilibili-starter/src/test/java/org/apache/flink/bilibili/starter/context.json",
			"/Users/haozhugogo/IdeaProjects/flink/flink-bilibili-starter/src/test/java/org/apache/flink/bilibili/starter/flink-conf.yaml"});
	}
}
