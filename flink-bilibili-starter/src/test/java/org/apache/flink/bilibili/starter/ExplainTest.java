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
class ExplainTest {

	@Test
	public void explain() throws Exception {

		String jar0 = "file:///Users/galaxy/Desktop/flink1.11_udf_lye.jar";
//		String jar1 = "file:///Users/galaxy/Desktop/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
//		String jar2 = "file:///Users/galaxy/Desktop/flink-dist_2.11-1.11.4-SNAPSHOT.jar";
		String jar1 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-bilibili-starter/target/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
		String jar2 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-dist/target/flink-dist_2.11-1.11.4-SNAPSHOT.jar";

		URL[] libs = new URL[] { new URL(jar0), new URL(jar1), new URL(jar2) };
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		Class c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
		Method method = c.getMethod("explain", String.class);

		JSONObject context = new JSONObject();
		context.put("sql", "CREATE FUNCTION MINUTE_ACC_SUM AS 'udf_agg.MinuteAccSum' LANGUAGE JAVA;\n"
			+ "CREATE FUNCTION STRING_SPLIT AS 'udf_udtf.StringSplit' LANGUAGE JAVA;\n" + "\n"
			+ "CREATE TABLE source_click(\n" + "  r_type integer,\n" + "  plat integer,\n"
			+ "  avid bigint,\n" + "  cid integer,\n" + "  part integer,\n" + "  mid integer,\n"
			+ "  lv integer,\n" + "  ftime varchar,\n" + "  stime varchar,\n" + "  did varchar,\n"
			+ "  ip varchar,\n" + "  ua varchar,\n" + "  buvid varchar,\n" + "  cookie_sid varchar,\n"
			+ "  refer varchar,\n" + "  type integer,\n" + "  sub_type integer,\n" + "  sid integer,\n"
			+ "  epid integer,\n" + "  play_mode integer,\n" + "  platform varchar,\n"
			+ "  device varchar,\n" + "  mobi_app varchar,\n" + "  auto_play integer,\n"
			+ "  session varchar,\n" + "  sdk varchar,\n" + "  build varchar\n" + ") WITH(\n"
			+ "  'connector' = 'bsql-kafka10',\n" + "  'offsetReset' = 'latest',\n"
			+ "  'topic' = 'lancer_main_report-click_archive-click-1',\n"
			+ "  'bootstrapServers' = '10.69.90.20:9092,10.69.90.21:9092,10.69.90.22:9092,10.69.91.11:9092,10.69.91.12:9092,10.69.91.13:9092,10.69.91.14:9092',\n"
			+ "  'bsql-delimit.delimiterKey' = '\\u0001'\n" + ");\n" + "\n" + "-- sink到tidb\n"
			+ "CREATE TABLE boss_total_vv(\n" + "  log_date varchar,\n" + "  log_minute varchar,\n"
			+ "  log_hour varchar,\n" + "  log_minutes varchar,\n" + "  total_vv bigint,\n"
			+ "  PRIMARY KEY(log_date, log_minute) NOT ENFORCED\n" + ") WITH(\n"
			+ "  'password' = 'RHS8NWs8vzbb/eUxoC2RsoJREsBxgRtnsqSz0q1KEWkFyB9XhhUJUZQJR909ddyF(已加密)',\n"
			+ "  'batchMaxTimeout' = '1000',\n" + "  'connectionPoolSize' = '1',\n"
			+ "  'connector' = 'bsql-mysql',\n" + "  'tableName' = 'boss_total_vv',\n"
			+ "  'batchSize' = '1000',\n"
			+ "  'url' = 'jdbc:mysql://datacenter-dwboss-4000.tidb.bilibili.co:4000/datacenter_dwboss?characterEncoding=utf8',\n"
			+ "  'userName' = 'datacenter_dwboss'\n" + ");\n" + "\n" + "-- 清洗、过滤脏数据\n"
			+ "CREATE VIEW view_etl_1 AS\n" + "SELECT\n" + "  avid,\n" + "  buvid,\n" + "  stime,\n"
			+ "  FROM_UNIXTIME10(COALESCE(stime, ''), 'yyyyMMdd') as log_date,\n"
			+ "  FROM_UNIXTIME10(COALESCE(stime, ''), 'HHmm') as log_minute\n" + "FROM\n"
			+ "  source_click\n" + "WHERE\n" + "  r_type = 1\n" + "  AND LENGTH(stime) = 10\n"
			+ "  AND stime >= '1612454400'\n"
			+ "  AND stime <= SUBSTR(CAST(UNIX_TIMESTAMP() AS varchar), 1, 10)\n"
			+ "  AND COALESCE(plat, -1) IN (0, 1, 2, 3, 4, 5)\n" + "  AND COALESCE(avid, 0) <> 0\n"
			+ "  AND COALESCE(cid, 0) <> 0\n" + "  AND COALESCE(lv, 0) IN (0, 1, 2, 3, 4, 5, 6)\n"
			+ "  AND COALESCE(type, 0) IN (0, 1, 2, 3, 4, 5, 10)\n"
			+ "  AND COALESCE(buvid, '') NOT IN (\n" + "    '9f89c84a559f573636a47ff8daed0d33',\n"
			+ "    'da4c50b4bab2fa1a3520e4482ab8e788',\n" + "    '0',\n" + "    ''\n" + "  );\n"
			+ "-- 记录最小时间、分钟增量\n" + "CREATE VIEW view_minute_vv AS\n" + "SELECT\n" + "  log_date,\n"
			+ "  start_minute,\n" + "  COUNT(1) AS vv_cnt_inc\n" + "FROM\n" + "  (\n" + "    SELECT\n"
			+ "      avid,\n" + "      buvid,\n" + "      stime,\n" + "      log_date,\n"
			+ "      MIN(log_minute) as start_minute\n" + "    FROM\n" + "      view_etl_1\n"
			+ "    GROUP BY\n" + "      buvid,\n" + "      avid,\n" + "      stime,\n"
			+ "      log_date\n" + "  ) t1\n" + "GROUP BY\n" + "  log_date,\n" + "  start_minute;\n"
			+ "-- 分钟累加、数据展开\n" + "CREATE VIEW result_table AS\n" + "SELECT\n"
			+ "  a.log_date AS log_date,\n" + "  split(b.min_and_vv, ':') [1] AS start_minute,\n"
			+ "  split(b.min_and_vv, ':') [2] AS vv_cnt\n" + "FROM\n" + "  (\n" + "    SELECT\n"
			+ "      log_date,\n"
			+ "      MINUTE_ACC_SUM(start_minute, vv_cnt_inc) AS vv_cnt_inc_result\n" + "    FROM\n"
			+ "      view_minute_vv\n" + "    GROUP BY\n" + "      log_date\n" + "  ) a,\n"
			+ "  LATERAL TABLE (STRING_SPLIT(a.vv_cnt_inc_result, ';')) AS b(min_and_vv);\n"
			+ "-- 拼接最终结果\n" + "CREATE VIEW final_result AS\n" + "SELECT\n"
			+ "  CONCAT_WS('_', log_date, start_minute) AS rk,\n" + "  log_date,\n" + "  start_minute,\n"
			+ "  vv_cnt\n" + "FROM\n" + "  result_table;\n" + "\n" + "/*\n" + "INSERT INTO STD_LOG\n"
			+ "SELECT\n" + "rk,\n" + "log_date,\n" + "start_minute,\n" + "vv_cnt\n"
			+ "FROM final_result;\n" + "*/\n" + "\n" + "-- 写入sink\n" + "INSERT INTO \n"
			+ "  boss_total_vv\n" + "SELECT\n" + "  log_date,\n" + "  start_minute AS log_minute,\n"
			+ "  SUBSTR(start_minute, 1, 2) AS log_hour,\n"
			+ "  SUBSTR(start_minute, 3, 2) AS log_minutes,\n" + "  CAST(vv_cnt AS bigint) AS total_vv\n"
			+ "FROM\n" + "  final_result;\n");

		context.put("sql", "CREATE TABLE BiliNewDevice (\nrequest_uri varchar,\ntime_iso varchar,\nip varchar,\nversion varchar,\nbuvid varchar,\nfts varchar,\nproid varchar,\nchid varchar,\npid varchar,\nbrand varchar,\ndeviceid varchar,\nmodel varchar,\nosver varchar,\nctime varchar,\nmid varchar,\nver varchar,\nnet varchar,\noid varchar,\nopenudid varchar,\nidfa varchar,\nmac varchar,\nbuvid_ext varchar,\nbuvid_old varchar,\noaid varchar,\nts AS PROCTIME()\n) WITH(\n-- 'parallelism' = '16',\n'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n'topic' = 'saber_kafka_sink_test',\n'offsetReset' = 'latest',\n'connector' = 'bsql-kafka10'\n);\nCREATE TABLE MyResult2 (\nPRIMARY KEY (test_1) NOT ENFORCED\n) WITH(\n'parallelism' = '10'\n)like Hive1.b_dwd.ods;\nINSERT INTO MyResult2 SELECT request_uri, request_uri, request_uri, request_uri, request_uri, request_uri, request_uri, request_uri from BiliNewDevice tmp;");
//		context.put("sql","INSERT INTO Hive1.b_dwd.ods SELECT ai, bi, ci, xx, ddd, xx, ai, bi from `Kafka_hw-sh-dc-lancer-kafka-ext`.`r_ods`.`ods_s_bigdata_dwd_to_bi_test_task_rt` tmp;");
		Object ret = method.invoke(c.newInstance(), context.toJSONString());

		System.out.println(ret);
	}

	@Test
	public void physicalPlan() throws Exception {

		String jar0 = "file:///Users/galaxy/Desktop/flink1.11_udf_lye.jar";
//		String jar1 = "file:///Users/galaxy/Desktop/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
//		String jar2 = "file:///Users/galaxy/Desktop/flink-dist_2.11-1.11.4-SNAPSHOT.jar";
		String jar1 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-bilibili-starter/target/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
		String jar2 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-dist/target/flink-dist_2.11-1.11.4-SNAPSHOT.jar";

		URL[] libs = new URL[] { new URL(jar0), new URL(jar1), new URL(jar2) };
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		Class c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
		Method method = c.getMethod("physicalPlan", String.class);

		JSONObject context = new JSONObject();
		context.put("sql", "CREATE FUNCTION MINUTE_ACC_SUM AS 'udf_agg.MinuteAccSum' LANGUAGE JAVA;\n"
			+ "CREATE FUNCTION STRING_SPLIT AS 'udf_udtf.StringSplit' LANGUAGE JAVA;\n" + "\n"
			+ "CREATE TABLE source_click(\n" + "  r_type integer,\n" + "  plat integer,\n"
			+ "  avid bigint,\n" + "  cid integer,\n" + "  part integer,\n" + "  mid integer,\n"
			+ "  lv integer,\n" + "  ftime varchar,\n" + "  stime varchar,\n" + "  did varchar,\n"
			+ "  ip varchar,\n" + "  ua varchar,\n" + "  buvid varchar,\n" + "  cookie_sid varchar,\n"
			+ "  refer varchar,\n" + "  type integer,\n" + "  sub_type integer,\n" + "  sid integer,\n"
			+ "  epid integer,\n" + "  play_mode integer,\n" + "  platform varchar,\n"
			+ "  device varchar,\n" + "  mobi_app varchar,\n" + "  auto_play integer,\n"
			+ "  session varchar,\n" + "  sdk varchar,\n" + "  build varchar\n" + ") WITH(\n"
			+ "  'connector' = 'bsql-kafka10',\n" + "  'offsetReset' = 'latest',\n"
			+ "  'topic' = 'lancer_main_report-click_archive-click-1',\n"
			+ "  'bootstrapServers' = '10.69.90.20:9092,10.69.90.21:9092,10.69.90.22:9092,10.69.91.11:9092,10.69.91.12:9092,10.69.91.13:9092,10.69.91.14:9092',\n"
			+ "  'bsql-delimit.delimiterKey' = '\\u0001'\n" + ");\n" + "\n" + "-- sink到tidb\n"
			+ "CREATE TABLE boss_total_vv(\n" + "  log_date varchar,\n" + "  log_minute varchar,\n"
			+ "  log_hour varchar,\n" + "  log_minutes varchar,\n" + "  total_vv bigint,\n"
			+ "  PRIMARY KEY(log_date, log_minute) NOT ENFORCED\n" + ") WITH(\n"
			+ "  'password' = 'RHS8NWs8vzbb/eUxoC2RsoJREsBxgRtnsqSz0q1KEWkFyB9XhhUJUZQJR909ddyF(已加密)',\n"
			+ "  'batchMaxTimeout' = '1000',\n" + "  'connectionPoolSize' = '1',\n"
			+ "  'connector' = 'bsql-mysql',\n" + "  'tableName' = 'boss_total_vv',\n"
			+ "  'batchSize' = '1000',\n"
			+ "  'url' = 'jdbc:mysql://datacenter-dwboss-4000.tidb.bilibili.co:4000/datacenter_dwboss?characterEncoding=utf8',\n"
			+ "  'userName' = 'datacenter_dwboss'\n" + ");\n" + "\n" + "-- 清洗、过滤脏数据\n"
			+ "CREATE VIEW view_etl_1 AS\n" + "SELECT\n" + "  avid,\n" + "  buvid,\n" + "  stime,\n"
			+ "  FROM_UNIXTIME10(COALESCE(stime, ''), 'yyyyMMdd') as log_date,\n"
			+ "  FROM_UNIXTIME10(COALESCE(stime, ''), 'HHmm') as log_minute\n" + "FROM\n"
			+ "  source_click\n" + "WHERE\n" + "  r_type = 1\n" + "  AND LENGTH(stime) = 10\n"
			+ "  AND stime >= '1612454400'\n"
			+ "  AND stime <= SUBSTR(CAST(UNIX_TIMESTAMP() AS varchar), 1, 10)\n"
			+ "  AND COALESCE(plat, -1) IN (0, 1, 2, 3, 4, 5)\n" + "  AND COALESCE(avid, 0) <> 0\n"
			+ "  AND COALESCE(cid, 0) <> 0\n" + "  AND COALESCE(lv, 0) IN (0, 1, 2, 3, 4, 5, 6)\n"
			+ "  AND COALESCE(type, 0) IN (0, 1, 2, 3, 4, 5, 10)\n"
			+ "  AND COALESCE(buvid, '') NOT IN (\n" + "    '9f89c84a559f573636a47ff8daed0d33',\n"
			+ "    'da4c50b4bab2fa1a3520e4482ab8e788',\n" + "    '0',\n" + "    ''\n" + "  );\n"
			+ "-- 记录最小时间、分钟增量\n" + "CREATE VIEW view_minute_vv AS\n" + "SELECT\n" + "  log_date,\n"
			+ "  start_minute,\n" + "  COUNT(1) AS vv_cnt_inc\n" + "FROM\n" + "  (\n" + "    SELECT\n"
			+ "      avid,\n" + "      buvid,\n" + "      stime,\n" + "      log_date,\n"
			+ "      MIN(log_minute) as start_minute\n" + "    FROM\n" + "      view_etl_1\n"
			+ "    GROUP BY\n" + "      buvid,\n" + "      avid,\n" + "      stime,\n"
			+ "      log_date\n" + "  ) t1\n" + "GROUP BY\n" + "  log_date,\n" + "  start_minute;\n"
			+ "-- 分钟累加、数据展开\n" + "CREATE VIEW result_table AS\n" + "SELECT\n"
			+ "  a.log_date AS log_date,\n" + "  split(b.min_and_vv, ':') [1] AS start_minute,\n"
			+ "  split(b.min_and_vv, ':') [2] AS vv_cnt\n" + "FROM\n" + "  (\n" + "    SELECT\n"
			+ "      log_date,\n"
			+ "      MINUTE_ACC_SUM(start_minute, vv_cnt_inc) AS vv_cnt_inc_result\n" + "    FROM\n"
			+ "      view_minute_vv\n" + "    GROUP BY\n" + "      log_date\n" + "  ) a,\n"
			+ "  LATERAL TABLE (STRING_SPLIT(a.vv_cnt_inc_result, ';')) AS b(min_and_vv);\n"
			+ "-- 拼接最终结果\n" + "CREATE VIEW final_result AS\n" + "SELECT\n"
			+ "  CONCAT_WS('_', log_date, start_minute) AS rk,\n" + "  log_date,\n" + "  start_minute,\n"
			+ "  vv_cnt\n" + "FROM\n" + "  result_table;\n" + "\n" + "/*\n" + "INSERT INTO STD_LOG\n"
			+ "SELECT\n" + "rk,\n" + "log_date,\n" + "start_minute,\n" + "vv_cnt\n"
			+ "FROM final_result;\n" + "*/\n" + "\n" + "-- 写入sink\n" + "INSERT INTO \n"
			+ "  boss_total_vv\n" + "SELECT\n" + "  log_date,\n" + "  start_minute AS log_minute,\n"
			+ "  SUBSTR(start_minute, 1, 2) AS log_hour,\n"
			+ "  SUBSTR(start_minute, 3, 2) AS log_minutes,\n" + "  CAST(vv_cnt AS bigint) AS total_vv\n"
			+ "FROM\n" + "  final_result;\n");

		context.put("sql", "CREATE TABLE BiliNewDevice (\nrequest_uri varchar,\ntime_iso varchar,\nip varchar,\nversion varchar,\nbuvid varchar,\nfts varchar,\nproid varchar,\nchid varchar,\npid varchar,\nbrand varchar,\ndeviceid varchar,\nmodel varchar,\nosver varchar,\nctime varchar,\nmid varchar,\nver varchar,\nnet varchar,\noid varchar,\nopenudid varchar,\nidfa varchar,\nmac varchar,\nbuvid_ext varchar,\nbuvid_old varchar,\noaid varchar,\nts AS PROCTIME()\n) WITH(\n-- 'parallelism' = '16',\n'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n'topic' = 'saber_kafka_sink_test',\n'offsetReset' = 'latest',\n'connector' = 'bsql-kafka10'\n);\nCREATE TABLE MyResult2 (\nPRIMARY KEY (test_1) NOT ENFORCED\n) WITH(\n'parallelism' = '10'\n)like Hive1.b_dwd.ods;\nINSERT INTO MyResult2 SELECT request_uri, request_uri, request_uri, request_uri, request_uri, request_uri, request_uri, request_uri from BiliNewDevice tmp;");

		Object ret = method.invoke(c.newInstance(), context.toJSONString());

		System.out.println(ret);
	}

	@Test
	public void lineAnalysis() throws Exception {

		String jar0 = "file:///Users/galaxy/Desktop/flink1.11_udf_lye.jar";
//		String jar1 = "file:///Users/galaxy/Desktop/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
//		String jar2 = "file:///Users/galaxy/Desktop/flink-dist_2.11-1.11.4-SNAPSHOT.jar";

		String jar1 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-bilibili-starter/target/flink-bilibili-starter-1.11.4-SNAPSHOT-jar-with-dependencies.jar";
		String jar2 = "file:///Users/haozhugogo/IdeaProjects/flink/flink-dist/target/flink-dist_2.11-1.11.4-SNAPSHOT.jar";

		URL[] libs = new URL[] { new URL(jar0), new URL(jar1), new URL(jar2) };
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		Class c = Class.forName("org.apache.flink.bilibili.starter.ToolBoxStarter");
		Method method = c.getMethod("lineAnalysis", String.class);

		JSONObject context = new JSONObject();
		context.put("sql", "CREATE FUNCTION MINUTE_ACC_SUM AS 'udf_agg.MinuteAccSum' LANGUAGE JAVA;\n"
			+ "CREATE FUNCTION STRING_SPLIT AS 'udf_udtf.StringSplit' LANGUAGE JAVA;\n" + "\n"
			+ "CREATE TABLE source_click(\n" + "  r_type integer,\n" + "  plat integer,\n"
			+ "  avid bigint,\n" + "  cid integer,\n" + "  part integer,\n" + "  mid integer,\n"
			+ "  lv integer,\n" + "  ftime varchar,\n" + "  stime varchar,\n" + "  did varchar,\n"
			+ "  ip varchar,\n" + "  ua varchar,\n" + "  buvid varchar,\n" + "  cookie_sid varchar,\n"
			+ "  refer varchar,\n" + "  type integer,\n" + "  sub_type integer,\n" + "  sid integer,\n"
			+ "  epid integer,\n" + "  play_mode integer,\n" + "  platform varchar,\n"
			+ "  device varchar,\n" + "  mobi_app varchar,\n" + "  auto_play integer,\n"
			+ "  session varchar,\n" + "  sdk varchar,\n" + "  build varchar\n" + ") WITH(\n"
			+ "  'connector' = 'bsql-kafka10',\n" + "  'offsetReset' = 'latest',\n"
			+ "  'topic' = 'lancer_main_report-click_archive-click-1',\n"
			+ "  'bootstrapServers' = '10.69.90.20:9092,10.69.90.21:9092,10.69.90.22:9092,10.69.91.11:9092,10.69.91.12:9092,10.69.91.13:9092,10.69.91.14:9092',\n"
			+ "  'bsql-delimit.delimiterKey' = '\\u0001'\n" + ");\n" + "\n" + "-- sink到tidb\n"
			+ "CREATE TABLE boss_total_vv(\n" + "  log_date varchar,\n" + "  log_minute varchar,\n"
			+ "  log_hour varchar,\n" + "  log_minutes varchar,\n" + "  total_vv bigint,\n"
			+ "  PRIMARY KEY(log_date, log_minute) NOT ENFORCED\n" + ") WITH(\n"
			+ "  'password' = 'RHS8NWs8vzbb/eUxoC2RsoJREsBxgRtnsqSz0q1KEWkFyB9XhhUJUZQJR909ddyF(已加密)',\n"
			+ "  'batchMaxTimeout' = '1000',\n" + "  'connectionPoolSize' = '1',\n"
			+ "  'connector' = 'bsql-mysql',\n" + "  'tableName' = 'boss_total_vv',\n"
			+ "  'batchSize' = '1000',\n"
			+ "  'url' = 'jdbc:mysql://datacenter-dwboss-4000.tidb.bilibili.co:4000/datacenter_dwboss?characterEncoding=utf8',\n"
			+ "  'userName' = 'datacenter_dwboss'\n" + ");\n" + "\n" + "-- 清洗、过滤脏数据\n"
			+ "CREATE VIEW view_etl_1 AS\n" + "SELECT\n" + "  avid,\n" + "  buvid,\n" + "  stime,\n"
			+ "  FROM_UNIXTIME10(COALESCE(stime, ''), 'yyyyMMdd') as log_date,\n"
			+ "  FROM_UNIXTIME10(COALESCE(stime, ''), 'HHmm') as log_minute\n" + "FROM\n"
			+ "  source_click\n" + "WHERE\n" + "  r_type = 1\n" + "  AND LENGTH(stime) = 10\n"
			+ "  AND stime >= '1612454400'\n"
			+ "  AND stime <= SUBSTR(CAST(UNIX_TIMESTAMP() AS varchar), 1, 10)\n"
			+ "  AND COALESCE(plat, -1) IN (0, 1, 2, 3, 4, 5)\n" + "  AND COALESCE(avid, 0) <> 0\n"
			+ "  AND COALESCE(cid, 0) <> 0\n" + "  AND COALESCE(lv, 0) IN (0, 1, 2, 3, 4, 5, 6)\n"
			+ "  AND COALESCE(type, 0) IN (0, 1, 2, 3, 4, 5, 10)\n"
			+ "  AND COALESCE(buvid, '') NOT IN (\n" + "    '9f89c84a559f573636a47ff8daed0d33',\n"
			+ "    'da4c50b4bab2fa1a3520e4482ab8e788',\n" + "    '0',\n" + "    ''\n" + "  );\n"
			+ "-- 记录最小时间、分钟增量\n" + "CREATE VIEW view_minute_vv AS\n" + "SELECT\n" + "  log_date,\n"
			+ "  start_minute,\n" + "  COUNT(1) AS vv_cnt_inc\n" + "FROM\n" + "  (\n" + "    SELECT\n"
			+ "      avid,\n" + "      buvid,\n" + "      stime,\n" + "      log_date,\n"
			+ "      MIN(log_minute) as start_minute\n" + "    FROM\n" + "      view_etl_1\n"
			+ "    GROUP BY\n" + "      buvid,\n" + "      avid,\n" + "      stime,\n"
			+ "      log_date\n" + "  ) t1\n" + "GROUP BY\n" + "  log_date,\n" + "  start_minute;\n"
			+ "-- 分钟累加、数据展开\n" + "CREATE VIEW result_table AS\n" + "SELECT\n"
			+ "  a.log_date AS log_date,\n" + "  split(b.min_and_vv, ':') [1] AS start_minute,\n"
			+ "  split(b.min_and_vv, ':') [2] AS vv_cnt\n" + "FROM\n" + "  (\n" + "    SELECT\n"
			+ "      log_date,\n"
			+ "      MINUTE_ACC_SUM(start_minute, vv_cnt_inc) AS vv_cnt_inc_result\n" + "    FROM\n"
			+ "      view_minute_vv\n" + "    GROUP BY\n" + "      log_date\n" + "  ) a,\n"
			+ "  LATERAL TABLE (STRING_SPLIT(a.vv_cnt_inc_result, ';')) AS b(min_and_vv);\n"
			+ "-- 拼接最终结果\n" + "CREATE VIEW final_result AS\n" + "SELECT\n"
			+ "  CONCAT_WS('_', log_date, start_minute) AS rk,\n" + "  log_date,\n" + "  start_minute,\n"
			+ "  vv_cnt\n" + "FROM\n" + "  result_table;\n" + "\n" + "/*\n" + "INSERT INTO STD_LOG\n"
			+ "SELECT\n" + "rk,\n" + "log_date,\n" + "start_minute,\n" + "vv_cnt\n"
			+ "FROM final_result;\n" + "*/\n" + "\n" + "-- 写入sink\n" + "INSERT INTO \n"
			+ "  boss_total_vv\n" + "SELECT\n" + "  log_date,\n" + "  start_minute AS log_minute,\n"
			+ "  SUBSTR(start_minute, 1, 2) AS log_hour,\n"
			+ "  SUBSTR(start_minute, 3, 2) AS log_minutes,\n" + "  CAST(vv_cnt AS bigint) AS total_vv\n"
			+ "FROM\n" + "  final_result;\n");

//		context.put("sql", "CREATE TABLE BiliNewDevice (\nrequest_uri varchar,\ntime_iso varchar,\nip varchar,\nversion varchar,\nbuvid varchar,\nfts varchar,\nproid varchar,\nchid varchar,\npid varchar,\nbrand varchar,\ndeviceid varchar,\nmodel varchar,\nosver varchar,\nctime varchar,\nmid varchar,\nver varchar,\nnet varchar,\noid varchar,\nopenudid varchar,\nidfa varchar,\nmac varchar,\nbuvid_ext varchar,\nbuvid_old varchar,\noaid varchar,\nts AS PROCTIME()\n) WITH(\n-- 'parallelism' = '16',\n'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n'topic' = 'saber_kafka_sink_test',\n'offsetReset' = 'latest',\n'connector' = 'bsql-kafka10'\n);\nCREATE TABLE MyResult2 (\nPRIMARY KEY (test_1) NOT ENFORCED\n) WITH(\n'parallelism' = '10'\n)like Hive1.b_dwd.ods;\nINSERT INTO MyResult2 SELECT request_uri, request_uri, request_uri, request_uri, request_uri, request_uri, request_uri, request_uri from BiliNewDevice tmp;");
//		context.put("sql","INSERT INTO Hive1.b_dwd.ods SELECT ai, bi, ci, xx, ddd, xx, ai, bi from `Kafka_hw-sh-dc-lancer-kafka-ext`.`r_ods`.`ods_s_bigdata_dwd_to_bi_test_task_rt` tmp;");
		context.put("sql","create table app_play_duration(\n" +
			"  stime BIGINT,\n" +
			"  build VARCHAR,\n" +
			"  buvid VARCHAR,\n" +
			"  mobi_app VARCHAR,\n" +
			"  platform VARCHAR,\n" +
			"  session VARCHAR,\n" +
			"  mid BIGINT,\n" +
			"  aid BIGINT,\n" +
			"  cid BIGINT,\n" +
			"  sid INT,\n" +
			"  epid INT,\n" +
			"  type INT,\n" +
			"  sub_type INT,\n" +
			"  quality INT,\n" +
			"  total_time BIGINT,\n" +
			"  paused_time INT,\n" +
			"  played_time INT,\n" +
			"  video_duration INT,\n" +
			"  play_type INT,\n" +
			"  network_type INT,\n" +
			"  last_play_progress_time INT,\n" +
			"  max_play_progress_time INT,\n" +
			"  play_mode INT,\n" +
			"  device VARCHAR,\n" +
			"  `from` VARCHAR,\n" +
			"  epid_status INT,\n" +
			"  play_status INT,\n" +
			"  user_status INT,\n" +
			"  actual_played_time INT,\n" +
			"  auto_play INT,\n" +
			"  detail_play_time INT,\n" +
			"  list_play_time BIGINT,\n" +
			"  from_spmid VARCHAR,\n" +
			"  spmid VARCHAR,\n" +
			"  sdk VARCHAR,\n" +
			"  miniplayer_play_time INT,\n" +
			"  ts as TO_TIMESTAMP_SAFE(stime*1000),\n" +
			"  WATERMARK FOR ts as ts - INTERVAL '1' SECOND\n" +
			") WITH(\n" +
			"  'bootstrapServers' = '10.69.90.20:9092,10.69.90.21:9092,10.69.90.22:9092,10.69.91.11:9092,10.69.91.12:9092,10.69.91.13:9092,10.69.91.14:9092',\n" +
			"  'topic' = 'lancer_main_report-click_app-play-duration',\n" +
			"  'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"  'offsetReset' = 'latest',\n" +
			"  'connector' = 'bsql-kafka10'\n" +
			");\n" +
			"-- 维表 测试账号\n" +
			"CREATE TABLE ups (\n" +
			"  id BIGINT,\n" +
			"  mid BIGINT,\n" +
			"  type BIGINT,\n" +
			"  note VARCHAR,\n" +
			"  uid BIGINT,\n" +
			"  ctime VARCHAR,\n" +
			"  mtime VARCHAR,\n" +
			"  state BIGINT,\n" +
			"  PRIMARY KEY (id,mid) NOT ENFORCED\n" +
			") WITH(\n" +
			"  'url' = 'jdbc:mysql://creative-2305-t-3440.dbdns.bilibili.co:3440/bilibili_manager?characterEncoding=utf8',\n" +
			"  'userName' = 'sqoop',\n" +
			"  'password' = 'iNjbO7raOPid/QdoK6B4llMb69L+CctaP49C+hqHfIkFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"  'tableName' = 'ups',\n" +
			"  'cache' = 'LRU',\n" +
			"  'cacheTTLMs' = '3600000',\n" +
			"  'connector' = 'bsql-mysql',\n" +
			"  'cacheSize' = '1000'\n" +
			");\n" +
			"-- 维表 有效稿件筛选\n" +
			"CREATE TABLE archive (\n" +
			"  id bigint,\n" +
			"  typeid bigint,\n" +
			"  state BIGINT,\n" +
			"  ctime TIMESTAMP,\n" +
			"  aid BIGINT,\n" +
			"  PRIMARY KEY (id,aid) NOT ENFORCED\n" +
			") WITH(\n" +
			"  'url' = 'jdbc:mysql://videoup-2306-t-3403.dbdns.bilibili.co:3403/bilibili_archive?characterEncoding=utf8',\n" +
			"  'userName' = 'sqoop',\n" +
			"  'password' = 'uSG7fP1maYqfb2k48kaKTjXjpn+UlQ54m9jhMEjgjAUFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"  'tableName' = 'archive',\n" +
			"  'cache' = 'LRU',\n" +
			"  'cacheTTLMs' = '3600000',  -- 毫秒\n" +
			"  'connector' = 'bsql-mysql',\n" +
			"  'cacheSize' = '100000',\n" +
			"  'connectionPoolSize' = '2'\n" +
			");\n" +
			"\n" +
			"-- app端清洗\\过滤\n" +
			"create view app as\n" +
			"select\n" +
			"ts,\n" +
			"  stime,\n" +
			"  buvid,\n" +
			"  platform,\n" +
			"  plattype,\n" +
			"  a.mid,\n" +
			"  avid,\n" +
			"  cast(sid as bigint) sid,\n" +
			"  cast(epid as bigint) epid,\n" +
			"  played_time\n" +
			"from\n" +
			"  (\n" +
			"    select\n" +
			"    ts,\n" +
			"      stime,\n" +
			"      buvid,\n" +
			"      platform,\n" +
			"    case\n" +
			"        when lower(platform) in ('android', 'ios')\n" +
			"        and mobi_app <> 'android_tv_yst' then 'app'\n" +
			"        when mobi_app = 'android_tv_yst' then 'ott'\n" +
			"      end as plattype,\n" +
			"      if(\n" +
			"        mid = ''\n" +
			"        or mid is null,\n" +
			"        0,\n" +
			"        mid\n" +
			"      ) as mid,\n" +
			"      aid as avid,\n" +
			"      sid,\n" +
			"      epid,\n" +
			"      played_time\n" +
			"    from\n" +
			"      app_play_duration\n" +
			"    where\n" +
			"      coalesce(platform, '') <> ''\n" +
			"      and coalesce(mobi_app, '') <> ''\n" +
			"      and coalesce(build, '') <> ''\n" +
			"      and stime > 0\n" +
			"      and length(cast(stime as varchar)) = 10\n" +
			"      and FROM_UNIXTIME(stime*1000, 'yyyyMMdd') >= DATE_FORMAT(\n" +
			"        TIMESTAMPADD(DAY, -1, CURRENT_TIMESTAMP),\n" +
			"        'yyyyMMdd'\n" +
			"      )\n" +
			"      and FROM_UNIXTIME((stime + total_time)*1000, 'yyyyMMdd') = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd')\n" +
			"      and played_time > 0\n" +
			"      and played_time < 86400 ---观看时长\n" +
			"      and paused_time >= 0 ---暂停时长\n" +
			"      and total_time > 0 --总时长 包含暂定，缓冲，播放\n" +
			"      and paused_time + played_time <= total_time\n" +
			"      and coalesce(buvid, '') <> ''\n" +
			"      and aid > 0\n" +
			"      and aid is not null\n" +
			"      and cid > 0\n" +
			"      and cid is not null\n" +
			"      and FROM_UNIXTIME(\n" +
			"        (stime + (\n" +
			"          case\n" +
			"            when list_play_time is null then 0\n" +
			"            else list_play_time\n" +
			"          end\n" +
			"        ))*1000,\n" +
			"        'yyyyMMdd'\n" +
			"      ) = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd')\n" +
			"  ) a\n" +
			"  JOIN archive FOR SYSTEM_TIME AS OF now() AS d1 ON a.avid = d1.aid\n" +
			"  LEFT JOIN ups FOR SYSTEM_TIME AS OF now() AS d2 ON a.avid = d2.mid and d2.type=72 and d2.state=1\n" +
			"WHERE\n" +
			"  d2.mid is null and a.sid>0 and a.epid>0;\n" +
			"\n" +
			"\n" +
			"\n" +
			"-- 写入下一层topic\n" +
			"  CREATE TABLE log_sink (\n" +
			"    stime BIGINT,\n" +
			"  buvid VARCHAR,\n" +
			"  platform VARCHAR,\n" +
			"  plattype VARCHAR,\n" +
			"  mid BIGINT,\n" +
			"  avid BIGINT,\n" +
			"  sid BIGINT,\n" +
			"  epid BIGINT,\n" +
			"  played_time BIGINT\n" +
			"    ) WITH(\n" +
			"        'connector' = 'bsql-kafka10',\n" +
			"        'topic' = 'r_rtd.rtd_flow_wbap_play_ogv_duration',\n" +
			"        'bootstrapServers' = 'jssz-middle-pakafka-01.host.bilibili.co:9092,jssz-middle-pakafka-02.host.bilibili.co:9092,jssz-middle-pakafka-03.host.bilibili.co:9092,jssz-middle-pakafka-04.host.bilibili.co:9092,jssz-middle-pakafka-05.host.bilibili.co:9092',\n" +
			"        'bsql-delimit.delimiterKey' = '\\u0001'\n" +
			"    );\n" +
			"--   ) WITH(\n" +
			"--     'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"--     'connector' = 'bsql-log',\n" +
			"--     'infinite' = 'true'\n" +
			"--   );\n" +
			"-- 最终结果\n" +
			"INSERT INTO\n" +
			"  log_sink\n" +
			"SELECT\n" +
			"  stime,\n" +
			"  buvid,\n" +
			"  platform,\n" +
			"  plattype,\n" +
			"  mid,\n" +
			"  avid,\n" +
			"  sid,\n" +
			"  epid,\n" +
			"  played_time\n" +
			"FROM\n" +
			"  app;");
		context.put("sql","    CREATE TABLE cpm_request_log(\n" +
			"        event_type varchar,\n" +
			"        room_id bigint,\n" +
			"        session_id varchar,\n" +
			"        group_id varchar,\n" +
			"        order_id varchar,\n" +
			"        parent_id bigint,\n" +
			"        area_id bigint,\n" +
			"        buvid varchar,\n" +
			"        event_ts varchar,\n" +
			"        mid bigint,\n" +
			"        order_type bigint,\n" +
			"        ratio double,\n" +
			"        channel_id bigint,\n" +
			"        creative_id BIGINT,\n" +
			"        suborder_id BIGINT\n" +
			"    ) WITH(\n" +
			"        'connector' = 'bsql-kafka10',\n" +
			"        'offsetReset' = 'latest',\n" +
			"        'topic' = 'lancer_jishubu_live_cpm_report_log',   'bootstrapServers'='10.69.179.17:9092,10.69.179.18:9092,10.69.179.19:9092,10.69.179.20:9092,10.69.181.30:9092,10.69.181.31:9092,10.69.181.32:9092,10.69.181.33:9092,10.70.38.11:9092,10.70.38.12:9092',\n" +
			"        'bsql-delimit.delimiterKey' = '\\u0001'\n" +
			"    );\n" +
			"-- 实时源，异常流\n" +
			"    CREATE TABLE cpm_request_fail_log(\n" +
			"        event_type varchar,\n" +
			"        room_id bigint,\n" +
			"        session_id varchar,\n" +
			"        group_id varchar,\n" +
			"        order_id varchar,\n" +
			"        parent_id bigint,\n" +
			"        area_id bigint,\n" +
			"        buvid varchar,\n" +
			"        event_ts varchar,\n" +
			"        mid bigint,\n" +
			"        order_type bigint,\n" +
			"        ratio double,\n" +
			"        channel_id bigint,\n" +
			"        creative_id BIGINT,\n" +
			"        suborder_id BIGINT\n" +
			"    ) WITH(\n" +
			"        'connector' = 'bsql-kafka10',\n" +
			"        'offsetReset' = 'latest',\n" +
			"        'topic' = 'r_ods.ods_live_cpm_request_log',   \n" +
			"        'bootstrapServers'='jssz-lowlevel-pakafka-01.host.bilibili.co:9092,jssz-lowlevel-pakafka-02.host.bilibili.co:9092,jssz-lowlevel-pakafka-03.host.bilibili.co:9092,jssz-lowlevel-pakafka-04.host.bilibili.co:9092,jssz-lowlevel-pakafka-05.host.bilibili.co:9092',\n" +
			"        'bsql-delimit.delimiterKey' = '\\u0001'\n" +
			"    );\n" +
			"-- sink topic表结构\n" +
			"    CREATE TABLE sink (\n" +
			"  request_id varchar,\n" +
			"  session_id varchar,\n" +
			"  buvid varchar,\n" +
			"  mid bigint,\n" +
			"  event_type varchar,\n" +
			"  event_ts varchar,\n" +
			"  event_time timestamp,\n" +
			"  group_id varchar,\n" +
			"  order_id bigint,\n" +
			"  order_type bigint,\n" +
			"  price bigint,\n" +
			"  online_status bigint,\n" +
			"  budget bigint,\n" +
			"  ratio double,\n" +
			"  area_id bigint,\n" +
			"  area_name varchar,\n" +
			"  subarea_id bigint,\n" +
			"  subarea_name varchar,\n" +
			"  ruid bigint,\n" +
			"  room_id bigint,\n" +
			"  anchor_nick varchar,\n" +
			"  account_id bigint,\n" +
			"  account_type bigint,\n" +
			"  channel_id bigint,\n" +
			"  account_name varchar\n" +
			"    ) WITH(\n" +
			"        'connector' = 'bsql-kafka10',\n" +
			"        'topic' = 'live_tpc_dwd_cpm_request_log',\n" +
			"        'bootstrapServers' = '10.69.142.34:9092,10.69.176.29:9092.10.69.176.30:9092,10.69.176.31:9092,10.69.176.32:9092',\n" +
			"        'bsql-delimit.delimiterKey' = '\\u0001'\n" +
			"    );\n" +
			"    -- ) WITH(\n" +
			"    -- 'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"    -- 'connector' = 'bsql-log',\n" +
			"    -- 'infinite' = 'true'\n" +
			"    -- );\n" +
			"-- -- 实时源，异常流\n" +
			"--     CREATE TABLE cpm_request_fail_log_sink(\n" +
			"--         event_type varchar,\n" +
			"--         room_id bigint,\n" +
			"--         session_id varchar,\n" +
			"--         group_id varchar,\n" +
			"--         order_id varchar,\n" +
			"--         parent_id bigint,\n" +
			"--         area_id bigint,\n" +
			"--         buvid varchar,\n" +
			"--         event_ts varchar,\n" +
			"--         mid bigint,\n" +
			"--         order_type bigint,\n" +
			"--         ratio double,\n" +
			"--         channel_id bigint,\n" +
			"--         creative_id BIGINT,\n" +
			"--         suborder_id BIGINT\n" +
			"--     -- ) WITH(\n" +
			"--     --     'connector' = 'bsql-kafka10',\n" +
			"--     --     'topic' = 'r_ods.ods_live_cpm_request_log',   \n" +
			"--     --     'bootstrapServers'='jssz-lowlevel-pakafka-01.host.bilibili.co:9092,jssz-lowlevel-pakafka-02.host.bilibili.co:9092,jssz-lowlevel-pakafka-03.host.bilibili.co:9092,jssz-lowlevel-pakafka-04.host.bilibili.co:9092,jssz-lowlevel-pakafka-05.host.bilibili.co:9092',\n" +
			"--     --     'bsql-delimit.delimiterKey' = '\\u0001'\n" +
			"--     -- );\n" +
			"--     ) WITH(\n" +
			"--     'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"--     'connector' = 'bsql-log',\n" +
			"--     'infinite' = 'true'\n" +
			"--     );\n" +
			"\n" +
			"\n" +
			"\n" +
			"\n" +
			"\n" +
			"---*** 定义数据维表，类型 hive/mysql ***--------\n" +
			"-- 1、channel_order\n" +
			"    CREATE TABLE channel_order ( \n" +
			"        id bigint,\n" +
			"        order_id bigint,\n" +
			"        order_type int, \n" +
			"        channel_id bigint,\n" +
			"        roomid bigint,\n" +
			"    PRIMARY KEY (id) NOT ENFORCED\n" +
			"    ) WITH( \n" +
			"    'url' = 'jdbc:mysql://xroom-feed-80975-t-3865.dbdns.bilibili.co:3865/xroomfeed?characterEncoding=utf8',\n" +
			"    'userName' = 'sqoop',\n" +
			"    'password' = 'hJSIKAMAwIs9TqfqKPpRwYgm0En0aD7sodEC3BXz/5kFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"    'tableName' = 'channel_order',\n" +
			"    'connector' = 'bsql-mysql',\n" +
			"    'cache' = 'LRU',\n" +
			"    'cacheTTLMs' = '3600000',\n" +
			"    'cacheSize' = '1000'\n" +
			"    );\n" +
			"-- 2、cpm_order\n" +
			"    CREATE TABLE cpm_order ( \n" +
			"        id bigint,\n" +
			"        coin_num bigint,\n" +
			"        account_id bigint,\n" +
			"        online_status bigint, -- 订单状态1：已完成 2：推广中\n" +
			"    PRIMARY KEY (id) NOT ENFORCED\n" +
			"    ) WITH( \n" +
			"    'url' = 'jdbc:mysql://xroom-feed-80975-t-3865.dbdns.bilibili.co:3865/xroomfeed?characterEncoding=utf8',\n" +
			"    'userName' = 'sqoop',\n" +
			"    'password' = 'hJSIKAMAwIs9TqfqKPpRwYgm0En0aD7sodEC3BXz/5kFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"    'tableName' = 'cpm_order',\n" +
			"    'connector' = 'bsql-mysql',\n" +
			"    'cache' = 'LRU',\n" +
			"    'cacheTTLMs' = '3600000',\n" +
			"    'cacheSize' = '1000'\n" +
			"    );\n" +
			"\n" +
			"-- 3、cpm_anchor_order\n" +
			"    CREATE TABLE cpm_anchor_order ( \n" +
			"        id bigint,\n" +
			"        coin_num bigint,\n" +
			"        online_status bigint, -- 订单状态1：推广中 2：已完成\n" +
			"        PRIMARY KEY (id) NOT ENFORCED\n" +
			"    ) WITH( \n" +
			"    'url' = 'jdbc:mysql://xroom-feed-80975-t-3865.dbdns.bilibili.co:3865/xroomfeed?characterEncoding=utf8',\n" +
			"    'userName' = 'sqoop',\n" +
			"    'password' = 'hJSIKAMAwIs9TqfqKPpRwYgm0En0aD7sodEC3BXz/5kFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"    'tableName' = 'cpm_anchor_order',\n" +
			"    'connector' = 'bsql-mysql',\n" +
			"    'cache' = 'LRU',\n" +
			"    'cacheTTLMs' = '3600000',\n" +
			"    'cacheSize' = '1000'\n" +
			"    );\n" +
			"\n" +
			"\n" +
			"-- 4、cpm_anchor_realtime_order\n" +
			"    CREATE TABLE cpm_anchor_realtime_order ( \n" +
			"        id bigint,\n" +
			"        coin_num bigint,\n" +
			"        online_status bigint, -- 订单状态1：推广中 2：已完成\n" +
			"        PRIMARY KEY (id) NOT ENFORCED\n" +
			"    ) WITH( \n" +
			"    'url' = 'jdbc:mysql://xroom-feed-80975-t-3865.dbdns.bilibili.co:3865/xroomfeed?characterEncoding=utf8',\n" +
			"    'userName' = 'sqoop',\n" +
			"    'password' = 'hJSIKAMAwIs9TqfqKPpRwYgm0En0aD7sodEC3BXz/5kFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"    'tableName' = 'cpm_anchor_realtime_order',\n" +
			"    'connector' = 'bsql-mysql',\n" +
			"    'cache' = 'LRU',\n" +
			"    'cacheTTLMs' = '3600000',\n" +
			"    'cacheSize' = '1000'\n" +
			"    );\n" +
			"-- 5、cpm_account_base_info\n" +
			"    CREATE TABLE cpm_account_base_info (\n" +
			"        account_id bigint,\n" +
			"        account_type bigint,\n" +
			"        account_name varchar,\n" +
			"        room_id bigint,\n" +
			"        g_id bigint,\n" +
			"        g_name varchar,\n" +
			"        primary_key varchar, \n" +
			"        PRIMARY KEY (room_id) NOT ENFORCED -- 注意这里的主键值，后续join条件只能以这个等值链接，不支持双值链接\n" +
			"    ) WITH(\n" +
			"        'path' = 'viewfs://jssz-bigdata-cluster/department/live/warehouse/cpm_account_base_info',\n" +
			"        'period' = 'day:04',\n" +
			"        'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"        'connector' = 'bsql-hdfs'\n" +
			"    );\n" +
			"-- 6、cpm_area_base_info\n" +
			"    CREATE TABLE cpm_area_base_info (\n" +
			"        area_id bigint,\n" +
			"        area_name varchar,\n" +
			"        subarea_id bigint,\n" +
			"        subarea_name varchar,\n" +
			"        PRIMARY KEY (subarea_id) NOT ENFORCED -- 注意这里的\n" +
			"    ) WITH(\n" +
			"        'path' = 'viewfs://jssz-bigdata-cluster/department/live/warehouse/cpm_area_base_info',\n" +
			"        'period' = 'day:02',\n" +
			"        'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"        'connector' = 'bsql-hdfs'\n" +
			"    );\n" +
			"-- 7、realtime_starlight_record\n" +
			"    CREATE TABLE realtime_starlight_record ( \n" +
			"        id bigint,\n" +
			"        remarks varchar,\n" +
			"        PRIMARY KEY (id) NOT ENFORCED\n" +
			"    ) WITH( \n" +
			"        'url' = 'jdbc:mysql://xroom-feed-80975-t-3865.dbdns.bilibili.co:3865/xroomfeed?characterEncoding=utf8',\n" +
			"        'userName' = 'sqoop',\n" +
			"        'password' = 'hJSIKAMAwIs9TqfqKPpRwYgm0En0aD7sodEC3BXz/5kFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
			"        'tableName' = ' realtime_starlight_record',\n" +
			"        'connector' = 'bsql-mysql',\n" +
			"        'cache' = 'LRU',\n" +
			"        'cacheTTLMs' = '3600000',\n" +
			"        'cacheSize' = '1000'\n" +
			"    );\n" +
			"\n" +
			"-- 8、account_base_info_pk\n" +
			"CREATE TABLE account_base_info_pk (\n" +
			"  account_id bigint,\n" +
			"  account_type bigint,\n" +
			"  account_name varchar,\n" +
			"  room_id bigint,\n" +
			"  g_id bigint,\n" +
			"  g_name varchar,\n" +
			"  primary_key varchar, \n" +
			"  PRIMARY KEY (primary_key) NOT ENFORCED -- 注意这里的主键值，后续join条件只能以这个等值链接，不支持双值链接\n" +
			") WITH(\n" +
			"  'path' = 'viewfs://jssz-bigdata-cluster/department/live/warehouse/cpm_account_base_info',\n" +
			"  'period' = 'day:04',\n" +
			"  'parallelism' = '10',\n" +
			"  'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"  'connector' = 'bsql-hdfs'\n" +
			");\n" +
			"\n" +
			"\n" +
			"-- views\n" +
			"-- union all合并异常流\n" +
			"    CREATE view t1 AS\n" +
			"    SELECT  a.event_type \n" +
			"        ,a.room_id \n" +
			"        ,a.session_id \n" +
			"        ,a.group_id \n" +
			"        ,a.order_id \n" +
			"        ,a.parent_id \n" +
			"        ,a.area_id \n" +
			"        ,a.buvid \n" +
			"        ,a.event_ts \n" +
			"        ,a.mid \n" +
			"        ,a.order_type \n" +
			"        ,a.ratio \n" +
			"        ,a.channel_id \n" +
			"        ,a.creative_id \n" +
			"        ,a.suborder_id\n" +
			"    FROM cpm_request_log a\n" +
			"    UNION ALL\n" +
			"    SELECT  a.event_type \n" +
			"        ,a.room_id \n" +
			"        ,a.session_id \n" +
			"        ,a.group_id \n" +
			"        ,a.order_id \n" +
			"        ,a.parent_id \n" +
			"        ,a.area_id \n" +
			"        ,a.buvid \n" +
			"        ,a.event_ts \n" +
			"        ,a.mid \n" +
			"        ,a.order_type \n" +
			"        ,a.ratio \n" +
			"        ,a.channel_id \n" +
			"        ,a.creative_id \n" +
			"        ,a.suborder_id\n" +
			"    FROM cpm_request_fail_log a;\n" +
			"-- left join channel_order,获取订单相关信息\n" +
			"    create view t2 as\n" +
			"    SELECT  a.event_type\n" +
			"        ,a.room_id\n" +
			"        ,a.session_id\n" +
			"        ,a.group_id\n" +
			"        ,b.order_id\n" +
			"        ,a.parent_id\n" +
			"        ,a.area_id\n" +
			"        ,a.buvid\n" +
			"        ,a.event_ts\n" +
			"        ,a.mid\n" +
			"        ,a.order_type\n" +
			"        ,a.ratio\n" +
			"        ,a.channel_id\n" +
			"        ,a.creative_id\n" +
			"        ,a.suborder_id\n" +
			"    FROM \n" +
			"    (\n" +
			"        SELECT  *\n" +
			"        FROM t1\n" +
			"        WHERE suborder_id<>0  \n" +
			"    ) a\n" +
			"    LEFT JOIN channel_order for SYSTEM_TIME AS OF now() AS b\n" +
			"    ON a.suborder_id = b.id \n" +
			"    UNION ALL\n" +
			"    SELECT  a.event_type\n" +
			"        ,a.room_id\n" +
			"        ,a.session_id\n" +
			"        ,a.group_id\n" +
			"        ,cast(a.order_id as bigint) order_id\n" +
			"        ,a.parent_id\n" +
			"        ,a.area_id\n" +
			"        ,a.buvid\n" +
			"        ,a.event_ts\n" +
			"        ,a.mid\n" +
			"        ,a.order_type\n" +
			"        ,a.ratio\n" +
			"        ,a.channel_id\n" +
			"        ,a.creative_id\n" +
			"        ,a.suborder_id\n" +
			"    FROM t1 a\n" +
			"    WHERE suborder_id=0;\n" +
			"\n" +
			"-- left join 房间维表和分区维表,获取相关信息\n" +
			"    create view t3 as\n" +
			"    select\n" +
			"    a.session_id session_id_org,\n" +
			"        if(\n" +
			"        split(coalesce(a.session_id, ''), '_') [2] is null,\n" +
			"        '',\n" +
			"        split(coalesce(a.session_id, ''), '_') [1]\n" +
			"    ) as request_id,\n" +
			"    if(\n" +
			"        split(coalesce(a.session_id, ''), '_') [2] is null,\n" +
			"        split(coalesce(a.session_id, ''), '_') [1],\n" +
			"        split(coalesce(a.session_id, ''), '_') [2]\n" +
			"    ) as session_id,\n" +
			"    a.buvid,\n" +
			"    a.mid,\n" +
			"    a.event_type,\n" +
			"    a.event_ts,\n" +
			"    TO_TIMESTAMP(cast(a.event_ts as bigint) * 1000) event_time,\n" +
			"    a.group_id,\n" +
			"    cast(a.order_id as bigint) order_id,\n" +
			"    a.order_type,\n" +
			"    a.ratio,\n" +
			"    a.parent_id area_id,\n" +
			"    b.area_name,\n" +
			"    a.area_id subarea_id,\n" +
			"    b.subarea_name,\n" +
			"    c.account_id ruid,\n" +
			"    a.room_id,\n" +
			"    c.account_name anchor_nick,\n" +
			"    a.order_type account_type,\n" +
			"    a.channel_id,\n" +
			"    a.suborder_id,\n" +
			"    a.creative_id\n" +
			"    from\n" +
			"    t2 a\n" +
			"    left join cpm_area_base_info for SYSTEM_TIME AS OF now() AS b \n" +
			"        on a.area_id = b.subarea_id\n" +
			"    left join cpm_account_base_info for SYSTEM_TIME AS OF now() AS c \n" +
			"        on a.room_id = c.room_id;\n" +
			"\n" +
			"-- left join 订单维表,获取订单状态和订单预算\n" +
			"    create view t4 as\n" +
			"    select\n" +
			"    session_id_org,\n" +
			"    request_id,\n" +
			"    session_id,\n" +
			"    buvid,\n" +
			"    mid,\n" +
			"    event_type,\n" +
			"    event_ts,\n" +
			"    event_time,\n" +
			"    group_id,\n" +
			"    order_id,\n" +
			"    order_type,\n" +
			"    price,\n" +
			"    online_status,\n" +
			"    budget,\n" +
			"    ratio,\n" +
			"    COALESCE(area_id,-9998) as area_id,\n" +
			"    COALESCE(area_name,'inner_ad') as area_name,\n" +
			"    COALESCE(subarea_id,-9998) as subarea_id,\n" +
			"    COALESCE(subarea_name,'inner_ad') as subarea_name,\n" +
			"    ruid,\n" +
			"    room_id,\n" +
			"    anchor_nick,\n" +
			"    account_id,\n" +
			"    account_name,\n" +
			"    account_type,\n" +
			"    channel_id,\n" +
			"    suborder_id,\n" +
			"    creative_id\n" +
			"    from(\n" +
			"    -- 主播侧\n" +
			"    select\n" +
			"      a.session_id_org,\n" +
			"        a.request_id,\n" +
			"        a.session_id,\n" +
			"        a.buvid,\n" +
			"        a.mid,\n" +
			"        a.event_type,\n" +
			"        a.event_ts,\n" +
			"        a.event_time,\n" +
			"        a.group_id,\n" +
			"        a.order_id,\n" +
			"        a.order_type,\n" +
			"        -99 price,\n" +
			"        b.online_status,\n" +
			"        b.coin_num budget,\n" +
			"        a.ratio,\n" +
			"        a.area_id,\n" +
			"        a.area_name,\n" +
			"        a.subarea_id,\n" +
			"        a.subarea_name,\n" +
			"        a.ruid,\n" +
			"        a.room_id,\n" +
			"        a.anchor_nick,\n" +
			"        a.account_id,\n" +
			"        '-99' account_name,\n" +
			"        a.account_type,\n" +
			"        a.channel_id,\n" +
			"        a.suborder_id,\n" +
			"      a.creative_id\n" +
			"    from(\n" +
			"        select\n" +
			"      session_id_org,\n" +
			"        request_id,\n" +
			"        session_id,\n" +
			"        buvid,\n" +
			"        mid,\n" +
			"        event_type,\n" +
			"        event_ts,\n" +
			"        event_time,\n" +
			"        group_id,\n" +
			"        order_id,\n" +
			"        order_type,\n" +
			"        ratio,\n" +
			"        area_id,\n" +
			"        area_name,\n" +
			"        subarea_id,\n" +
			"        subarea_name,\n" +
			"        ruid,\n" +
			"        room_id,\n" +
			"        anchor_nick,\n" +
			"        ruid account_id,\n" +
			"        account_type,\n" +
			"        channel_id,\n" +
			"        suborder_id,\n" +
			"      creative_id\n" +
			"        from t3 \n" +
			"        where order_type=3\n" +
			"    ) a left join cpm_anchor_order for SYSTEM_TIME AS OF now() AS b \n" +
			"        on a.order_id = b.id\n" +
			"    union all\n" +
			"    -- 公会侧 \n" +
			"    select \n" +
			"      a.session_id_org,\n" +
			"        a.request_id,\n" +
			"        a.session_id,\n" +
			"        a.buvid,\n" +
			"        a.mid,\n" +
			"        a.event_type,\n" +
			"        a.event_ts,\n" +
			"        a.event_time,\n" +
			"        a.group_id,\n" +
			"        a.order_id,\n" +
			"        a.order_type,\n" +
			"        -99 price,\n" +
			"        case when b.online_status=1 then 2\n" +
			"            when b.online_status=2 then 1 \n" +
			"            else -99\n" +
			"        end online_status,\n" +
			"        b.coin_num budget,\n" +
			"        a.ratio,\n" +
			"        a.area_id,\n" +
			"        a.area_name,\n" +
			"        a.subarea_id,\n" +
			"        a.subarea_name,\n" +
			"        a.ruid,\n" +
			"        a.room_id,\n" +
			"        a.anchor_nick,\n" +
			"        b.account_id,\n" +
			"        '-99' account_name,\n" +
			"        a.account_type,\n" +
			"        a.channel_id,\n" +
			"        a.suborder_id,\n" +
			"      a.creative_id\n" +
			"        from(select\n" +
			"             session_id_org,\n" +
			"                request_id,\n" +
			"                session_id,\n" +
			"                buvid,\n" +
			"                mid,\n" +
			"                event_type,\n" +
			"                event_ts,\n" +
			"                event_time,\n" +
			"                group_id,\n" +
			"                order_id,\n" +
			"                order_type,\n" +
			"                ratio,\n" +
			"                area_id,\n" +
			"                area_name,\n" +
			"                subarea_id,\n" +
			"                subarea_name,\n" +
			"                ruid,\n" +
			"                room_id,\n" +
			"                anchor_nick,\n" +
			"                account_type,\n" +
			"                channel_id,\n" +
			"                suborder_id,\n" +
			"             creative_id\n" +
			"            from t3 \n" +
			"            where order_type=2) a \n" +
			"    left join cpm_order for SYSTEM_TIME AS OF now() AS b \n" +
			"        on a.order_id = b.id\n" +
			"    union all \n" +
			"    -- 实时星光侧\n" +
			"    select \n" +
			"      a.session_id_org,\n" +
			"        a.request_id,\n" +
			"        a.session_id,\n" +
			"        a.buvid,\n" +
			"        a.mid,\n" +
			"        a.event_type,\n" +
			"        a.event_ts,\n" +
			"        a.event_time,\n" +
			"        a.group_id,\n" +
			"        a.order_id,\n" +
			"        a.order_type,\n" +
			"        -99 price,\n" +
			"        b.online_status,\n" +
			"        b.coin_num budget,\n" +
			"        a.ratio,\n" +
			"        a.area_id,\n" +
			"        a.area_name,\n" +
			"        a.subarea_id,\n" +
			"        a.subarea_name,\n" +
			"        a.ruid,\n" +
			"        a.room_id,\n" +
			"        a.anchor_nick,\n" +
			"        a.account_id,\n" +
			"        c.remarks as account_name,\n" +
			"        a.account_type,\n" +
			"        a.channel_id,\n" +
			"        a.suborder_id,\n" +
			"      a.creative_id\n" +
			"        from(select\n" +
			"             session_id_org,\n" +
			"                request_id,\n" +
			"                session_id,\n" +
			"                buvid,\n" +
			"                mid,\n" +
			"                event_type,\n" +
			"                event_ts,\n" +
			"                event_time,\n" +
			"                group_id,\n" +
			"                order_id,\n" +
			"                order_type,\n" +
			"                ratio,\n" +
			"                area_id,\n" +
			"                area_name,\n" +
			"                subarea_id,\n" +
			"                subarea_name,\n" +
			"                ruid,\n" +
			"                room_id,\n" +
			"                ruid account_id,\n" +
			"                anchor_nick,\n" +
			"                account_type,\n" +
			"                channel_id,\n" +
			"                suborder_id,\n" +
			"             creative_id\n" +
			"            from t3 \n" +
			"            where order_type=5) a \n" +
			"    left join cpm_anchor_realtime_order for SYSTEM_TIME AS OF now() AS b \n" +
			"        on a.order_id = b.id\n" +
			"    LEFT JOIN realtime_starlight_record for SYSTEM_TIME AS OF now() AS c\n" +
			"        ON a.order_id=c.id\n" +
			"    ) a ;\n" +
			"\n" +
			"\n" +
			"-- left join 账户维表,获取账户名称\n" +
			"create view t5 as\n" +
			"select \n" +
			"        a.session_id_org,\n" +
			"        a.request_id,\n" +
			"        a.session_id,\n" +
			"        a.buvid,\n" +
			"        a.mid,\n" +
			"        a.event_type,\n" +
			"        a.event_ts,\n" +
			"        a.event_time,\n" +
			"        a.group_id,\n" +
			"        a.order_id,\n" +
			"        a.order_type,\n" +
			"        a.price,\n" +
			"        a.online_status,\n" +
			"        a.budget,\n" +
			"        a.ratio,\n" +
			"        a.area_id,\n" +
			"        a.area_name,\n" +
			"        a.subarea_id,\n" +
			"        a.subarea_name,\n" +
			"        a.ruid,\n" +
			"        a.room_id,\n" +
			"        a.anchor_nick,\n" +
			"        a.account_id,\n" +
			"        case when a.account_type in (2,3) then b.account_name \n" +
			"       when a.account_type=5 then a.account_name else '-99'\n" +
			"       end as account_name,\n" +
			"        a.account_type,\n" +
			"        a.channel_id,\n" +
			"        a.suborder_id,\n" +
			"        a.creative_id\n" +
			"        from(select\n" +
			"                session_id_org,\n" +
			"request_id,\n" +
			"session_id,\n" +
			"buvid,\n" +
			"mid,\n" +
			"event_type,\n" +
			"event_ts,\n" +
			"event_time,\n" +
			"group_id,\n" +
			"order_id,\n" +
			"order_type,\n" +
			"price,\n" +
			"online_status,\n" +
			"budget,\n" +
			"ratio,\n" +
			"area_id,\n" +
			"area_name,\n" +
			"subarea_id,\n" +
			"subarea_name,\n" +
			"ruid,\n" +
			"room_id,\n" +
			"anchor_nick,\n" +
			"account_id,\n" +
			"account_name,\n" +
			"account_type,\n" +
			"channel_id,\n" +
			"suborder_id,\n" +
			"creative_id,\n" +
			"                concat(cast(account_id as string),cast(account_type as string)) pkey\n" +
			"            from t4) a \n" +
			"    LEFT JOIN account_base_info_pk for SYSTEM_TIME AS OF now() AS b\n" +
			"ON a.pkey=b.primary_key;\n" +
			"\n" +
			"\n" +
			"\n" +
			"\n" +
			"-- 最终结果写入sink表\n" +
			"insert into\n" +
			"  sink\n" +
			"select\n" +
			"    request_id,\n" +
			"    session_id,\n" +
			"    buvid,\n" +
			"    mid,\n" +
			"    event_type,\n" +
			"    event_ts,\n" +
			"    event_time,\n" +
			"    group_id,\n" +
			"    order_id,\n" +
			"    order_type,\n" +
			"    price,\n" +
			"    online_status,\n" +
			"    budget,\n" +
			"    ratio,\n" +
			"    area_id,\n" +
			"    area_name,\n" +
			"    subarea_id,\n" +
			"    subarea_name,\n" +
			"    ruid,\n" +
			"    room_id,\n" +
			"    anchor_nick,\n" +
			"    account_id,\n" +
			"    account_type,\n" +
			"    channel_id,\n" +
			"    account_name\n" +
			"from\n" +
			"  t5;");
		Object ret = method.invoke(c.newInstance(), context.toJSONString());

		System.out.println(ret);
	}
}
