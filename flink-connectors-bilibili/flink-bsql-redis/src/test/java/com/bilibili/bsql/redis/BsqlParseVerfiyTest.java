/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.junit.Test;

import static org.apache.flink.table.runtime.operators.join.latency.config.GlobalConfiguration.*;
import static org.apache.flink.table.runtime.operators.join.latency.config.GlobalConfiguration.LATENCY_REDIS_BUCKET_SIZE_CONFIG;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlParseVerfiyTest.java, v 0.1 2020-09-29 18:20
zhouxiaogang Exp $$
 */
public class BsqlParseVerfiyTest {

	public static Method getDeclaredMethod(Object object, String methodName, Class<?> ... parameterTypes){
		Method method = null ;

		for(Class<?> clazz = object.getClass() ; clazz != Object.class ; clazz = clazz.getSuperclass()) {
			try {
				method = clazz.getDeclaredMethod(methodName, parameterTypes) ;
				return method ;
			} catch (Exception e) {
				//do nothing then can get method from super Class
			}
		}

		return null;
	}

	@Test
	public void newTest() throws Exception {
		ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();

		String sql =
			"CREATE FUNCTION encode_play_state_from_bytes AS 'com.bilibili.label.join.base.udf.EncodePlayStateFromBytes' LANGUAGE JAVA; " +
				"CREATE FUNCTION encode_action_state_from_bytes AS 'com.bilibili.label.join.base.udf.EncodeActionStateFromBytes' LANGUAGE JAVA; " +
				"CREATE FUNCTION encode_click_state_from_bytes AS 'com.bilibili.label.join.base.udf.EncodeClickStateFromBytes' LANGUAGE JAVA; " +
				"CREATE FUNCTION cast_string_to_bigint AS 'com.bilibili.label.join.base.udf.CastStringToBigint' LANGUAGE JAVA; " +
				"CREATE FUNCTION merged_feed_unfold AS 'com.bilibili.label.join.base.udf.MergedFeedUnfold' LANGUAGE JAVA; " +
				"CREATE FUNCTION encode_show_state_from_bytes AS 'com.bilibili.label.join.base.udf.EncodeShowStateFromBytes' LANGUAGE JAVA; " +
				"CREATE FUNCTION cast_string_to_int AS 'com.bilibili.label.join.base.udf.CastStringToInt' LANGUAGE JAVA; " +
				"CREATE FUNCTION durations_to_object AS 'com.bilibili.label.join.base.udf.PlayDurationsToDataExchangeObject' LANGUAGE JAVA; " +
				"CREATE FUNCTION clean_str_4_hvtftb AS 'com.bilibili.label.join.base.udf.CleanStringForHiveTextFileTable' LANGUAGE JAVA; " ;

			String a =	"CREATE TABLE click ( " +
				"  ip VARCHAR, " +
				"  ctime VARCHAR, " +
				"  api VARCHAR, " +
				"  buvid VARCHAR, " +
				"  mid VARCHAR, " +
				"  client VARCHAR, " +
				"  itemid VARCHAR, " +
				"  displayid VARCHAR, " +
				"  err_code VARCHAR, " +
				"  from_section VARCHAR, " +
				"  build VARCHAR, " +
				"  trackid VARCHAR, " +
				"  auto_play VARCHAR, " +
				"  from_spmid VARCHAR, " +
				"  spmid VARCHAR, " +
				"  ts AS PROCTIME() " +
				") WITH ( " +
				"  'parallelism' = '30', " +
				"  'bootstrapServers' = '10.69.90.20:9092,10.69.90.21:9092,10.69.90.22:9092', " +
				"  'topic' = 'lancer_main_app_click-api-1', " +
				"  'bsql-delimit.delimiterKey' = '\\u0001', " +
				"  'offsetReset' = 'latest', " +
				"  'connector' = 'bsql-kafka10' " +
				") " ;

		String b ="CREATE TABLE show_ ( " +
				"  ip VARCHAR, " +
				"  ctime VARCHAR, " +
				"  api VARCHAR, " +
				"  buvid VARCHAR, " +
				"  mid VARCHAR, " +
				"  client VARCHAR, " +
				"  pagetype VARCHAR, " +
				"  showlist VARCHAR, " +
				"  displayid VARCHAR, " +
				"  is_rec VARCHAR, " +
				"  build VARCHAR, " +
				"  return_code VARCHAR, " +
				"  user_feature VARCHAR, " +
				"  zoneid VARCHAR, " +
				"  adresponse VARCHAR, " +
				"  deviceid VARCHAR, " +
				"  network VARCHAR, " +
				"  new_user VARCHAR, " +
				"  flush VARCHAR, " +
				"  autoplay_card VARCHAR, " +
				"  trackid VARCHAR, " +
				"  device_type VARCHAR, " +
				"  banner_show_case VARCHAR, " +
				"  ts AS PROCTIME() " +
				") WITH ( " +
				"  'parallelism' = '40', " +
				"  'bootstrapServers' = '10.69.90.20:9092,10.69.90.21:9092,10.69.90.22:9092', " +
				"  'topic' = 'lancer_main_app_app-feed', " +
				"  'bsql-delimit.delimiterKey' = '\\u0001', " +
				"  'offsetReset' = 'latest', " +
				"  'connector' = 'bsql-kafka10' " +
				") ";

		String c ="CREATE TABLE redis_duration_last ( " +
				"  key VARCHAR, " +
				"  val VARCHAR, " +
				"  PRIMARY KEY (key) NOT ENFORCED " +
				") WITH ( " +
				"  'redistype' = '3', " +
				"  'parallelism' = '300', " +
				"  'connector' = 'bsql-redis', " +
				"  'url' = '10.69.192.36:6814,10.69.192.37:6814,10.69.192.38:6812' " +
				") " ;

		String d ="CREATE TABLE redis_duration_max ( " +
				"  key VARCHAR, " +
				"  val VARCHAR, " +
				"  PRIMARY KEY (key) NOT ENFORCED " +
				") WITH ( " +
				"  'redistype' = '3', " +
				"  'parallelism' = '300', " +
				"  'connector' = 'bsql-redis', " +
				"  'url' = '10.69.192.36:6838,10.69.192.37:6838,10.69.192.38:6836' " +
				") " ;

			String dd ="CREATE TABLE redis_state ( " +
				"  key VARCHAR, " +
				"  val BYTES, " +
				"  PRIMARY KEY (key) NOT ENFORCED " +
				") WITH ( " +
				"  'redistype' = '3', " +
				"  'parallelism' = '300', " +
				"  'connector' = 'bsql-redis', " +
				"  'url' = '10.69.3.22:6816,10.69.3.22:6823,10.69.12.28:6817,10.69.12.28:6820,10.69.3.22:6816,10.70.23.19:6849' " +
				") " ;

			String e ="CREATE TABLE sink ( " +
				"  show_timestamp BIGINT, " +
				"  mid BIGINT, " +
				"  buvid VARCHAR, " +
				"  item_type INT, " +
				"  item_raw_feature VARCHAR, " +
				"  mid_raw_feature VARCHAR, " +
				"  itemid BIGINT, " +
				"  from_section INT, " +
				"  label INT, " +
				"  click_state VARCHAR, " +
				"  show_state VARCHAR, " +
				"  action_state VARCHAR, " +
				"  play_state VARCHAR, " +
				"  duration_map VARCHAR, " +
				"  rt_state_map VARCHAR, " +
				"  trackid VARCHAR " +
				") WITH ( " +
				"  'parallelism' = '300', " +
				"  'bootstrapServers' = '10.70.144.26:9092,10.70.144.28:9092,10.70.144.33:9092,10.70.145.11:9092,10.70.145.13:9092', " +
				"  'topic' = 'ai_tpc_pegasus_label_join_rtstate', " +
				"  'bsql-delimit.delimiterKey' = '\\u0001', " +
				"  'connector' = 'bsql-kafka10' " +
				") " ;

				String f ="CREATE VIEW show_filtered AS " +
				"SELECT " +
				"  ctime AS show_timestamp, " +
				"  mid AS mid, " +
				"  buvid AS buvid, " +
				"  user_feature AS mid_raw_feature, " +
				"  showlist AS showlist, " +
				"  trackid AS trackid, " +
				"  ts AS ts, " +
				"  CONCAT('click_24h_140:', mid) AS state_key_click, " +
				"  CONCAT('show_24h_1600:', mid) AS state_key_show, " +
				"  CONCAT('action_24h_110:', mid) AS state_key_action, " +
				"  CONCAT('play_24h_290:', mid) AS state_key_play " +
				"FROM " +
				"  show_ " +
				"WHERE " +
				"  TRIM(api) = '/x/feed/index' " +
				"  AND return_code = '0' " +
				"  AND buvid <> '0' " +
				"  AND buvid <> '' " +
				"  AND mid <> '' " ;

					String g ="CREATE VIEW joined_click_state AS " +
				"SELECT " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.mid_raw_feature, " +
				"  a.showlist, " +
				"  a.trackid, " +
				"  a.ts, " +
				"  a.state_key_show, " +
				"  a.state_key_action, " +
				"  a.state_key_play, " +
				"  encode_click_state_from_bytes(b.val) AS click_state " +
				"FROM " +
				"  show_filtered a " +
				"  LEFT JOIN redis_state FOR SYSTEM_TIME AS OF now() AS b ON a.state_key_click = b.key ";

		String h ="CREATE VIEW joined_show_state AS " +
				"SELECT " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.mid_raw_feature, " +
				"  a.showlist, " +
				"  a.trackid, " +
				"  a.ts, " +
				"  a.click_state, " +
				"  a.state_key_action, " +
				"  a.state_key_play, " +
				"  encode_show_state_from_bytes(b.val) AS show_state " +
				"FROM " +
				"  joined_click_state a " +
				"  LEFT JOIN redis_state FOR SYSTEM_TIME AS OF now() AS b ON a.state_key_show = b.key ";

		String i ="CREATE VIEW joined_action_state AS " +
				"SELECT " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.mid_raw_feature, " +
				"  a.showlist, " +
				"  a.trackid, " +
				"  a.ts, " +
				"  a.click_state, " +
				"  a.show_state, " +
				"  a.state_key_play, " +
				"  encode_action_state_from_bytes(b.val) AS action_state " +
				"FROM " +
				"  joined_show_state a " +
				"  LEFT JOIN redis_state FOR SYSTEM_TIME AS OF now() AS b ON a.state_key_action = b.key ";

		String j ="CREATE VIEW joined_play_state AS " +
				"SELECT " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.mid_raw_feature, " +
				"  a.showlist, " +
				"  a.trackid, " +
				"  a.ts, " +
				"  a.click_state, " +
				"  a.show_state, " +
				"  a.action_state, " +
				"  encode_play_state_from_bytes(b.val) AS play_state " +
				"FROM " +
				"  joined_action_state a " +
				"  LEFT JOIN redis_state FOR SYSTEM_TIME AS OF now() AS b ON a.state_key_play = b.key " ;

		String k ="CREATE VIEW click_filtered AS " +
				"SELECT " +
				"  trackid AS trackid, " +
				"  itemid AS itemid, " +
				"  from_section AS from_section, " +
				"  ts AS ts " +
				"FROM " +
				"  click " +
				"WHERE " +
				"  from_section IN ('76', '761', '7') " +
				"  AND buvid <> '0' " +
				"  AND buvid <> '' " +
				"  AND mid <> '' " ;

		String l ="CREATE VIEW joined_click AS " +
				"SELECT " +
				"  a.trackid, " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.mid_raw_feature, " +
				"  a.click_state, " +
				"  a.show_state, " +
				"  a.action_state, " +
				"  a.play_state, " +
				"  a.showlist, " +
				"  b.itemid AS itemids, " +
				"  b.from_section AS from_sections " +
				"FROM " +
				"  joined_play_state a " +
				"LEFT JOIN click_filtered b ON a.trackid = b.trackid " +
				"  AND a.ts BETWEEN b.ts - INTERVAL '60' MINUTE AND b.ts " +
				"  AND b.ts BETWEEN a.ts - INTERVAL '60' MINUTE AND a.ts " ;

		String m ="CREATE VIEW unfold AS " +
				"SELECT " +
				"  a.trackid, " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  item.type_ AS item_type, " +
				"  item.raw_feature AS item_raw_feature, " +
				"  a.mid_raw_feature, " +
				"  item.id_ AS itemid, " +
				"  item.from_section AS from_section, " +
				"  item.label AS label, " +
				"  a.click_state, " +
				"  a.show_state, " +
				"  a.action_state, " +
				"  a.play_state, " +
				"  CONCAT( " +
				"    'PD_', " +
				"    CASE WHEN a.mid <> '0' THEN a.mid ELSE a.buvid END, " +
				"    '_', " +
				"    item.id_ " +
				"  ) AS duration_key_last, " +
				"  CONCAT( " +
				"    CASE WHEN a.mid <> '0' THEN a.mid ELSE a.buvid END, " +
				"    '_', " +
				"    item.id_ " +
				"  ) AS duration_key_max " +
				"FROM " +
				"  joined_click a, " +
				"  LATERAL TABLE(merged_feed_unfold(showlist, itemids, from_sections)) AS item( " +
				"    raw_feature, " +
				"    id_, " +
				"    type_, " +
				"    label, " +
				"    from_section " +
				") " ;

		String n ="CREATE VIEW joined_last AS " +
				"SELECT " +
				"  a.trackid, " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.item_type, " +
				"  a.item_raw_feature, " +
				"  a.mid_raw_feature, " +
				"  a.itemid, " +
				"  a.from_section, " +
				"  a.label, " +
				"  a.click_state, " +
				"  a.show_state, " +
				"  a.action_state, " +
				"  a.play_state, " +
				"  a.duration_key_max, " +
				"  CASE " +
				"    WHEN b.val IS NULL THEN '' " +
				"    ELSE CONCAT('l_', b.val) " +
				"  END AS last_val " +
				"FROM " +
				"  unfold a " +
				"  LEFT JOIN redis_duration_last FOR SYSTEM_TIME AS OF now() AS b ON a.duration_key_last = b.key " ;

		String o ="CREATE VIEW joined_max AS " +
				"SELECT " +
				"  a.trackid, " +
				"  a.show_timestamp, " +
				"  a.mid, " +
				"  a.buvid, " +
				"  a.item_type, " +
				"  a.item_raw_feature, " +
				"  a.mid_raw_feature, " +
				"  a.itemid, " +
				"  a.from_section, " +
				"  a.label, " +
				"  a.click_state, " +
				"  a.show_state, " +
				"  a.action_state, " +
				"  a.play_state, " +
				"  a.last_val, " +
				"  CASE " +
				"    WHEN b.val IS NULL THEN '' " +
				"    ELSE CONCAT('m_', b.val) " +
				"  END AS max_val " +
				"FROM " +
				"  joined_last a " +
				"  LEFT JOIN redis_duration_max FOR SYSTEM_TIME AS OF now() AS b ON a.duration_key_max = b.key " ;

		String[] sentence = sql.split(";");
		/**
		 * udf part
		 * */
		List<URL> jarFiles = new ArrayList<>();
		jarFiles.add(new File("/Users/zhouxiaogang/Downloads/label-join-base.jar").getAbsoluteFile().toURI().toURL());
		for (URL url : jarFiles) {
			Method method = getDeclaredMethod(threadClassLoader, "addURL", URL.class);
			method.setAccessible(true);
			method.invoke(threadClassLoader, url);
		}

		Configuration flinkConfig = new Configuration();
		flinkConfig.set(LATENCY_REDIS_CONFIG, "MOCK_TO_PASS_VALIDATE");
		flinkConfig.set(LATENCY_REDIS_TYPE_CONFIG, LATENCY_REDIS_TYPE_CONFIG.defaultValue());
		flinkConfig.set(LATENCY_COMPRESS_TYPE_CONFIG, LATENCY_COMPRESS_TYPE_CONFIG.defaultValue());
		flinkConfig.set(LATENCY_REDIS_BUCKET_SIZE_CONFIG, LATENCY_REDIS_BUCKET_SIZE_CONFIG.defaultValue());
		TableConfig customTableConfig = new TableConfig();
		customTableConfig.addConfiguration(flinkConfig);

		StreamExecutionEnvironment engineRuntime = StreamExecutionEnvironment.getExecutionEnvironment();
		engineRuntime.setParallelism(100);
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment engineTableRuntime = StreamTableEnvironmentImpl.create(engineRuntime, settings, customTableConfig);
//		Context localContext = Context.from("{}");
//		localContext.set("jobId", "ai-join");
//		localContext.set("raw[0].mainArgs",
//			"--redisAddr 10.69.73.23:6800,10.69.73.24:6800,10.69.73.25:6800,10.69.73.26:6800,10.69.73.27:6800 --redisBucket 12344 --redisType 1");
//		localContext.set("raw[0].bsqlCode", sql);
//
//		localContext = Context.from(localContext.toJSON());
//		localContext.set(PluginContextConstant.ENGINE_RUNTIME, engineRuntime);
//		localContext.set(PluginContextConstant.ENGINE_TABLE_RUNTIME, engineTableRuntime);
//		localContext.set(PluginContextConstant.CUSTOM_LOADER, customLoader);
//
		StatementSet statementSet = engineTableRuntime.createStatementSet();
//		localContext.set(PluginContextConstant.STATEMENT_SET, statementSet);

		/**
		 * below is the processor part, mimic the yarn Starter
		 * */

//        engineTableRuntime.executeSql(" CREATE FUNCTION now1 AS 'com.bilibili.saber.engine.flink.bsql.udf.udf.scalar.Now' LANGUAGE JAVA");
//		BSQLNativeProcessor BSQLProcessor = new BSQLNativeProcessor();
//		BSQLProcessor.initialize(localContext);
		for(String s: sentence) {
			if (s.length() < 5) {
				continue;
			}
			engineTableRuntime.executeSql(s);

			System.out.println(s);
		}
		engineTableRuntime.executeSql(a);
		engineTableRuntime.executeSql(b);
		engineTableRuntime.executeSql(c);
		engineTableRuntime.executeSql(d);
		engineTableRuntime.executeSql(dd);
		engineTableRuntime.executeSql(e);
		engineTableRuntime.executeSql(f);
		engineTableRuntime.executeSql(g);
		engineTableRuntime.executeSql(h);
		engineTableRuntime.executeSql(i);
		engineTableRuntime.executeSql(j);
		engineTableRuntime.executeSql(k);
		engineTableRuntime.executeSql(l);
		engineTableRuntime.executeSql(m);
		engineTableRuntime.executeSql(n);
		engineTableRuntime.executeSql(o);


		statementSet.addInsertSql("INSERT INTO " +
			"  sink " +
			"SELECT " +
			"  cast_string_to_bigint(show_timestamp) AS show_timestamp, " +
			"  cast_string_to_bigint(mid) AS mid, " +
			"  clean_str_4_hvtftb(buvid) AS buvid, " +
			"  cast_string_to_int(item_type) AS item_type, " +
			"  clean_str_4_hvtftb(item_raw_feature) AS item_raw_feature, " +
			"  clean_str_4_hvtftb(mid_raw_feature) AS mid_raw_feature, " +
			"  cast_string_to_bigint(itemid) AS itemid, " +
			"  cast_string_to_int(from_section) AS from_section, " +
			"  cast_string_to_int(label) AS label, " +
			"  clean_str_4_hvtftb(click_state) AS click_state, " +
			"  clean_str_4_hvtftb(show_state) AS show_state, " +
			"  clean_str_4_hvtftb(action_state) AS action_state, " +
			"  clean_str_4_hvtftb(play_state) AS play_state, " +
			"  durations_to_object(last_val, max_val) AS duration_map, " +
			"  '' AS rt_state_map, " +
			"  clean_str_4_hvtftb(trackid) AS trackid " +
			"FROM " +
			"  joined_max");

		Arrays.stream(engineTableRuntime.listTables()).forEach(System.err::println);

		statementSet.explain();
	}


	@Test
	public void testMultiKeyVarchar() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is blink) indicates whether the " +
				"example uses flink planner or blink planner.");
			return;
		}

		tEnv.executeSql("CREATE TABLE MyUserTable ( " +
			"  -- declare the schema of the table " +
			"  `userss` as 'aa1,,bb', " +
			"  `product` STRING, " +
			"  `amount` INTEGER " +
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-datagen', " +
			"  'rows-per-second' = '1' " +
			")");

		tEnv.executeSql("CREATE TABLE kfcSide ( " +
			"  -- declare the schema of the table " +
			"  user1 STRING, " +
			"  address ARRAY<STRING>, " +
			"PRIMARY KEY(user1)  NOT ENFORCED "+
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-redis', " +
			"  'redistype' = '3', " +
			"  'delimitKey' = ',', " +
			"  'url' = '172.22.33.30:7899' " +
			")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss, r.address[3] as product,a.amount   FROM MyUserTable as a" +
			" left join kfcSide FOR SYSTEM_TIME AS OF now() AS r " +
			" on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

//	@Test
	/**
	 * no need to support for the redis multi key with binary return
	 * */
	public void testMultiKeyBinary() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is blink) indicates whether the " +
				"example uses flink planner or blink planner.");
			return;
		}

		tEnv.executeSql("CREATE TABLE MyUserTable ( " +
			"  -- declare the schema of the table " +
			"  `userss` as 'aa1,bb', " +
			"  `product` STRING, " +
			"  `amount` INTEGER " +
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-datagen', " +
			"  'rows-per-second' = '1' " +
			")");

		tEnv.executeSql("CREATE TABLE kfcSide ( " +
			"  -- declare the schema of the table " +
			"  user1 STRING, " +
			"  address ARRAY<BYTES>, " +
			"PRIMARY KEY(user1)  NOT ENFORCED "+
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-redis', " +
			"  'redistype' = '3', " +
			"  'delimitKey' = ',', " +
			"  'url' = '172.22.33.30:7899' " +
			")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss, DECODE(r.address[2], 'UTF-8') as product,a.amount   FROM MyUserTable as a" +
			" left join kfcSide FOR SYSTEM_TIME AS OF now() AS r " +
			" on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

	@Test
	public void testVarcharArray() throws Exception {

		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is blink) indicates whether the " +
				"example uses flink planner or blink planner.");
			return;
		}

		tEnv.executeSql("CREATE TABLE MyUserTable ( " +
			"  -- declare the schema of the table " +
			"  `userss` as 'cc', " +
			"  `product` STRING, " +
			"  `amount` INTEGER " +
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-datagen', " +
			"  'rows-per-second' = '1' " +
			")");

		tEnv.executeSql("CREATE TABLE kfcSide ( " +
			"  -- declare the schema of the table " +
			"  user1 STRING, " +
			"  address STRING, " +
			"PRIMARY KEY(user1)  NOT ENFORCED "+
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-redis', " +
			"  'redistype' = '3', " +
			"  'url' = '172.22.33.30:7899' " +
			")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss, r.address as product,a.amount   FROM MyUserTable as a" +
			" left join kfcSide FOR SYSTEM_TIME AS OF now() AS r " +
			" on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

	@Test
	public void testBinaryArray() throws Exception {

		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is blink) indicates whether the " +
				"example uses flink planner or blink planner.");
			return;
		}



		tEnv.executeSql("CREATE TABLE MyUserTable ( " +
			"  -- declare the schema of the table " +
			"  `userss` as 'aa1', " +
			"  `product` STRING, " +
			"  `amount` INTEGER " +
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-datagen', " +
			"  'rows-per-second' = '1' " +
			")");


		tEnv.executeSql("CREATE TABLE RedisSide ( " +
			"  -- declare the schema of the table " +
			"  user1 STRING, " +
			"  address BYTES, " +
			"PRIMARY KEY(user1)  NOT ENFORCED "+
			") WITH ( " +
			"  -- declare the external system to connect to " +
			"  'connector' = 'bsql-redis', " +
			"  'redistype' = '3', " +
			"  'url' = '172.22.33.30:7899' " +
			")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss,  DECODE(r.address, 'UTF-8') as product,a.amount   FROM MyUserTable as a" +
			" left join " +
			"RedisSide FOR SYSTEM_TIME AS OF now() AS r " +
			"on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}



	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO.
	 */
	/**
	 * Simple POJO.
	 */
	public static class Order {
		public String userss;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(String user, String product, int amount) {
			this.userss = user;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + userss +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}
	}
}
