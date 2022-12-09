package com.bilibili.bsql.mysql;

import com.bilibili.bsql.common.global.SymbolsConstant;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.bilibili.bsql.common.keys.TableInfoKeys.NOT_NULL_ENFORCER;
import static org.apache.flink.table.api.ExplainDetail.CHANGELOG_MODE;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlMysqlTest.java
 * @description This is the description of BsqlMysqlTest.java
 * @createTime 2020-10-22 15:29:00
 */
public class BsqlMysqlTest {

	@Test
	public void mysqlSinkDeleteTest() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv;
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();
		tEnv = StreamTableEnvironment.create(env, settings);

		tEnv.executeSql("create function now_ts as 'com.bilibili.bsql.mysql.NowTs' language java");

		tEnv.executeSql("CREATE TABLE source_test (\n" +
			"  `name` STRING,\n" +
			"  `num` INT,\n" +
			"  `xtime` as proctime()\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '5'\n" +
			")");

		tEnv.executeSql("CREATE TABLE sink_test (\n" +
			" `name` STRING,\n" +
			" `num` BIGINT,\n" +
			" `mtime` timestamp(3), \n" +
			" primary key(name) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-mysql',\n" +
			"'url'='jdbc:mysql://localhost:3306/csz_test?characterEncoding=utf8&useSSL=false'," +
			"'userName'='root'," +
			"'password'='root'," +
			"'enableCompression' = 'true',\n" +
			"'batchSize'='10'," +
			"'batchMaxTimeout'='5000'," +
			"'enableDelete'='false'," +
			"'tableName'='test_sink_delete2'" +
			")");

		StatementSet stateSet =tEnv.createStatementSet();
		stateSet.addInsertSql("INSERT INTO sink_test " +
			"select cast(num as string) name, count(name) num , now_ts() mtime from (" +
			"SELECT substring(name,0, 1) name, count(1) num " +
			"FROM source_test " +
			"group by substring(name,0, 1)" +
				") group by num");

		stateSet.execute("mysqlSinkDeleteTest");

		while (true) {
			Thread.sleep(1000L);
		}

	}

    @Test
    public void mysqlTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        tEnv = StreamTableEnvironment.create(env, settings);


        tEnv.executeSql("CREATE TABLE source_test (\n" +
                "  `id` int,\n" +
                "  `message` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-kafka10',\n" +
                "  'topic' = 'meimeizitest1',\n" +
                "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
                "  'bsql-delimit.delimiterKey' = '|'" +
                ")");
        tEnv.executeSql("CREATE TABLE sink_test (\n" +
                "  `id` int,\n" +
                "  `message` STRING,\n" +
                "  primary key(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-mysql',\n" +
                "'url'='jdbc:mysql://localhost:3306/meimeizi_test?characterEncoding=utf8&useSSL=false'," +
                "'userName'='root'," +
                "'password'='rootroot'," +
                "'enableCompression' = 'true',\n" +
                "'batchSize'='100'," +
                //"'batchMaxTimeout'='5000'," +
                "'tableName'='test_key1'" +
                ")");

        StatementSet stateSet =tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test SELECT * FROM source_test");

        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }

    }

	@Test
    public void mysqlSinkObjectReuseTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        tEnv = StreamTableEnvironment.create(env, settings);
		// object reuse
		tEnv.getConfig().getConfiguration().setBoolean("pipeline.object-reuse", true);

		tEnv.executeSql("CREATE TABLE source_test(id INT, message STRING) WITH (\n" +
			"  'rows-per-second' = '3',\n" +
			"  'connector' = 'bsql-datagen'\n" +
			")");

		tEnv.executeSql("CREATE TABLE sink_test_log (\n" +
			"  id INT,\n" +
			"  `message` BIGINT\n" +
			") WITH('connector' = 'bsql-log', 'infinite' = 'true')");

		tEnv.executeSql("CREATE TABLE sink_test (\n" +
			"  `id` INT,\n" +
			"  `message` BIGINT,\n" +
			"  primary key(id) NOT ENFORCED\n" +
			") WITH (\n" +
			"  'connector' = 'bsql-mysql',\n" +
			"'url'='jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=false'," +
			"'userName'='root'," +
			"'password'='root'," +
			"'batchSize'='5'," +
			"'batchMaxTimeout'='10000'," +
			"'tableName'='mysql_sink_test'" +
			")");

        StatementSet stateSet =tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test_log SELECT id, UNIX_TIMESTAMP() as message FROM source_test");
        stateSet.addInsertSql("INSERT INTO sink_test SELECT id, UNIX_TIMESTAMP() as message FROM source_test");

        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }

    private static String hump2Line(String str) {
        String newStr = str.replaceAll("[A-Z]", "_$0").toLowerCase();
        while (newStr.startsWith(SymbolsConstant.UNDER_LINE)) {
            newStr = newStr.substring(1, newStr.length());
        }
        return newStr;
    }

    @Test
    public void sinkDateTest() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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

        tEnv.executeSql("create function now_ts as 'com.bilibili.bsql.mysql.NowTs' language java");
        tEnv.executeSql("create function to_unixtime as 'com.bilibili.bsql.mysql.ToUnixtime' language java");
        tEnv.executeSql("create function IS_NUMBER as 'com.bilibili.bsql.mysql.IsNumber' language java");
        tEnv.executeSql("create function print as 'com.bilibili.bsql.mysql.Print' language java");
        tEnv.executeSql("create function to_timestamp as 'com.bilibili.bsql.mysql.ToTimestamp' language java");

        tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
                "  `name` VARCHAR,\n" +
                "  `num` int,\n" +
                "  `xtime` TIMESTAMP,\n" +
                "WATERMARK  FOR xtime as xtime- INTERVAL '1' SECOND \n"+
                ") WITH (\n" +
                "  'connector' = 'bsql-kafka10',\n" +
                "  'topic' = 'flink_sql_kafka_sink',\n" +
                "  'offsetReset' = 'latest',\n" +
                "  'bootstrapServers' = '127.0.0.1:9092',\n" +
                "  'bsql-delimit.delimiterKey' = '|'   -- declare a format for this system\n" +
                ")");

        /*tEnv.executeSql("CREATE TABLE Source (\n" +
                "  -- declare the schema of the table\n" +
                "  `name` STRING,\n" +
                "  `num` INT,\n" +
                "  `xtime` as proctime()\n" +
				*//*"  `xtime` as now_ts(),\n"+
				"  WATERMARK FOR xtime as xtime - INTERVAL '10.000' SECOND\n"+*//*
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-datagen',\n" +
                "  'rows-per-second' = '1'\n" +
                ")");
*/
        tEnv.executeSql("CREATE TABLE my_date_table (\n" +
                "  `info_index` bigint,\n" +
                "  `courier_id` int,\n" +
                "  `city_id` STRING,\n" +
                "  PRIMARY KEY (city_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-mysql',\n" +
                "'url'='jdbc:mysql://127.0.0.1:3306/saber?characterEncoding=utf8&useSSL=false'," +
                "'userName'='root'," +
                "'password'='123456'," +
                "'batchMaxTimeout'='1000'," +
                "'tableName'='wxm_ck_test'" +
                ")");

        Table table = tEnv.sqlQuery("select * from my_kafka_test");

        tEnv.executeSql("insert into my_date_table select count(num) as info_index,sum(num) as courier_id,name from " + table + " group by name");
        tEnv.toRetractStream(table, Row.class).print("===== ");
        env.execute("");
    }

    @Test
    public void sinkRetract() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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

        System.out.println("======" + NOT_NULL_ENFORCER.defaultValue());

//		env.setStreamTimeCharacteristic(EventTime);
        tEnv.executeSql("create function now_ts as 'com.bilibili.bsql.mysql.NowTs' language java");

        tEnv.executeSql("CREATE TABLE Source (\n" +
                "  -- declare the schema of the table\n" +
                "  `name` STRING,\n" +
                "  `num` INT,\n" +
                "  `xtime` as proctime()\n" +
				/*"  `xtime` as now_ts(),\n"+
				"  WATERMARK FOR xtime as xtime - INTERVAL '10.000' SECOND\n"+*/
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-datagen',\n" +
                "  'rows-per-second' = '1'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
                "  -- declare the schema of the table\n" +
                "  `num` INT,\n" +
                "  `xtime` TIMESTAMP(3),\n" +
                "PRIMARY KEY(num) NOT ENFORCED" +
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-mysql',\n" +
                "'url'='jdbc:mysql://127.0.0.1:3306/wxm_test_mysql?characterEncoding=utf8&useSSL=false'," +
                "'userName'='root'," +
                "'password'='123456'," +
                "'tableName'='wxm_ck_test'" +
                ")");

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("insert into MyUserTable " +
                "SELECT max(num),TUMBLE_START(xtime, INTERVAL '20' SECOND) " +
                "FROM Source group by TUMBLE(xtime, INTERVAL '20' SECOND)");

        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void mysqlSideTest() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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


        String sql = "CREATE TABLE wxm_test (`userss` BIGINT, `product` BIGINT, `amount` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
        tEnv.executeSql(sql);

        Table table2 = tEnv.sqlQuery("select * from wxm_test");

        String mysqlDdl = "CREATE TABLE wxm_ck_test (info_index BIGINT,courier_id BIGINT,city_id BIGINT, xtime TIMESTAMP," +
                "PRIMARY KEY(info_index,courier_id) NOT ENFORCED" +
                ") WITH (" +
                "'connector'='bsql-mysql'," +
                "'url'='jdbc:mysql://127.0.0.1:3306/wxm_test?characterEncoding=utf8&useSSL=false'," +
                "'userName'='root'," +
                "'password'='123456'," +
                "'tableName'='wxm_ck_test'," +
                "'cache'='LRU'," +
                "'cacheSize'='10000'," +
                "'cacheTTLMs'='60000'" +
                ")";
        tEnv.executeSql(mysqlDdl);

     /*   String mysqlDdl2 = "CREATE TABLE wxm_ck_test_2 (info_index BIGINT,courier_id BIGINT,city_id BIGINT," +
                "PRIMARY KEY(info_index,courier_id) NOT ENFORCED" +
                ") WITH (" +
                "'connector'='bsql-mysql'," +
                "'url'='jdbc:mysql://127.0.0.1:3306/wxm_test_mysql?characterEncoding=utf8&useSSL=false'," +
                "'userName'='root'," +
                "'password'='123456'," +
                "'tableName'='wxm_ck_test'," +
                "'cache'='LRU'," +
                "'cacheSize'='10000'," +
                "'cacheTTLMs'='60000'" +
                ")";
        tEnv.executeSql(mysqlDdl2);*/


        Table clickHouseTable = tEnv.sqlQuery("select a.userss,b.info_index,b.xtime from wxm_test a " +
                "left join wxm_ck_test FOR SYSTEM_TIME AS OF now() AS b " +
                "on a.userss = b.info_index");

/*        String select = "select c.info_index,c.courier_id,c.city_id from \n" +
                "  (select b.info_index,b.courier_id,b.city_id from wxm_test a left join wxm_ck_test FOR SYSTEM_TIME AS OF now() AS b on a.userss = b.info_index) c left join wxm_ck_test_2 FOR SYSTEM_TIME AS OF now() AS d on c.info_index = d.info_index and c.courier_id = d.courier_id";*/

        /*       Table clickHouseTable = tEnv.sqlQuery(select);*/

        tEnv.toRetractStream(clickHouseTable, RowData.class).print("===== ");
        env.execute("");
    }

    @Test
    public void explanTest() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;

        TableEnvironmentImpl bbTableEnv = TableEnvironmentImpl.create(EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build());

        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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

        String DDL1 = "CREATE TABLE live_tpc_dwd_flow ( \n" +
                "ptime TIMESTAMP,\n" +
                "c_time TIMESTAMP,\n" +
                "s_time TIMESTAMP,\n" +
                "event_time TIMESTAMP,\n" +
                "fts TIMESTAMP,\n" +
                "guid VARCHAR,\n" +
                "mid BIGINT,\n" +
                "buvid VARCHAR,\n" +
                "pid INT,\n" +
                "ip VARCHAR,\n" +
                "country VARCHAR,\n" +
                "province VARCHAR,\n" +
                "city VARCHAR,\n" +
                "city_level VARCHAR,\n" +
                "source_id VARCHAR,\n" +
                "area_id INT,\n" +
                "subarea_id INT,\n" +
                "up_id BIGINT,\n" +
                "up_level INT,\n" +
                "room_id BIGINT,\n" +
                "up_session VARCHAR,\n" +
                "session_id VARCHAR,\n" +
                "event_id VARCHAR,\n" +
                "event_category INT,\n" +
                "event_id_from VARCHAR,\n" +
                "brand VARCHAR,\n" +
                "model VARCHAR,\n" +
                "osver VARCHAR,\n" +
                "version VARCHAR,\n" +
                "version_code INT,\n" +
                "ua VARCHAR,\n" +
                "network VARCHAR,\n" +
                "oid VARCHAR,\n" +
                "oidname VARCHAR,\n" +
                "pv_duration BIGINT,\n" +
                "pvstart BIGINT,\n" +
                "pvend BIGINT,\n" +
                "page_type INT,\n" +
                "play_type VARCHAR,\n" +
                "play_url VARCHAR,\n" +
                "refer_url VARCHAR,\n" +
                "appid INT,\n" +
                "launch_id BIGINT,\n" +
                "jumpfrom VARCHAR,\n" +
                "org_jumpfrom VARCHAR,\n" +
                "device_type INT,\n" +
                "watch_time INT,\n" +
                "log_src VARCHAR,\n" +
                "WATERMARK FOR event_time as event_time - INTERVAL '10.000' SECOND\n" +
                ") WITH( \n" +
                "'parallelism' = '18',\n" +
                "'bootstrapServers' = '10.70.141.32:9092,10.70.141.33:9092,10.70.141.34:9092,10.70.142.11:9092,10.70.142.12:9092',\n" +
                "'topic' = 'live_tpc_dwd_flow',\n" +
                "'bsql-delimit.delimiterKey' = '\\u0001',\n" +
                "'offsetReset' = 'latest',\n" +
                "'connector' = 'bsql-kafka10'\n" +
                ")";
        String DDL8 =
                "CREATE TABLE live_tpc_dws_flow ( \n" +
                        "window_end TIMESTAMP,\n" +
                        "buvid VARCHAR,\n" +
                        "mid BIGINT,\n" +
                        "brand VARCHAR,\n" +
                        "model VARCHAR,\n" +
                        "city_name VARCHAR,\n" +
                        "city_level VARCHAR,\n" +
                        "country VARCHAR,\n" +
                        "province VARCHAR,\n" +
                        "jumpfrom VARCHAR,\n" +
                        "pid INT,\n" +
                        "platform VARCHAR,\n" +
                        "app_version VARCHAR,\n" +
                        "event_id VARCHAR,\n" +
                        "room_id BIGINT,\n" +
                        "up_id BIGINT,\n" +
                        "area_id INT,\n" +
                        "subarea_id INT,\n" +
                        "log_src VARCHAR,\n" +
                        "pv BIGINT,\n" +
                        "min_time TIMESTAMP,\n" +
                        "max_time TIMESTAMP,\n" +
                        "area_type INT,\n" +
                        "resource_name_click VARCHAR,\n" +
                        "resource_name_show VARCHAR,\n" +
                        "jumpfrom_division VARCHAR,\n" +
                        "jumpfrom_category VARCHAR,\n" +
                        "jumpfrom_class VARCHAR,\n" +
                        "launch_id BIGINt,\n" +
                        "event_category INT\n" +
                        ") WITH( \n" +
                        "'parallelism' = '8',\n" +
                        "'bootstrapServers' = '10.70.141.32:9092,10.70.141.33:9092,10.70.141.34:9092,10.70.142.11:9092,10.70.142.12:9092',\n" +
                        "'topic' = 'live_tpc_dws_flow',\n" +
                        "'bsql-delimit.delimiterKey' = '\\u0001',\n" +
                        "'connector' = 'bsql-log',\n" +
                        "'infinite' = 'true'\n" +
                        ")";

        String DDL2 = "CREATE TABLE resource_dim ( \n" +
                "page_name varchar,\n" +
                "resource_name varchar,\n" +
                "event_show varchar,\n" +
                "event_click varchar,\n" +
                "jumpfrom varchar,\n" +
                "version varchar,\n" +
                "note varchar,\n" +
                "PRIMARY KEY (event_show, event_click) NOT ENFORCED\n" +
                ") WITH( \n" +
                "'password' = 'duIkE7lesyjVpk0fG4lmtzfDXkTSm1pQpeOaBLTe1BsFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
                "'cache' = 'LRU',\n" +
                "'cacheTTLMs' = '3600000',\n" +
                "'parallelism' = '18',\n" +
                "'connector' = 'bsql-mysql',\n" +
                "'tableName' = 'dim_app_resource_config_info',\n" +
                "'cacheSize' = '1000',\n" +
                "'url' = 'jdbc:mysql://live-user-service-34548-w-3628.dbdns.bilibili.co:3628/live_dimension?characterEncoding=utf8',\n" +
                "'userName' = 'live_dimension_61'\n" +
                ")";

        String DDL3 = "CREATE TABLE jumpfrom_dim ( \n" +
                "jumpfrom_id varchar,\n" +
                "jumpfrom_division varchar,\n" +
                "jumpfrom_category varchar,\n" +
                "jumpfrom_class varchar,\n" +
                "jumpfrom_remark varchar,\n" +
                "PRIMARY KEY (jumpfrom_id) NOT ENFORCED\n" +
                ") WITH( \n" +
                "'password' = 'duIkE7lesyjVpk0fG4lmtzfDXkTSm1pQpeOaBLTe1BsFyB9XhhUJUZQJR909ddyF(已加密)',\n" +
                "'cache' = 'LRU',\n" +
                "'cacheTTLMs' = '3600000',\n" +
                "'parallelism' = '18',\n" +
                "'connector' = 'bsql-mysql',\n" +
                "'tableName' = 'dim_jumpfrom_info_new',\n" +
                "'cacheSize' = '1000',\n" +
                "'url' = 'jdbc:mysql://live-user-service-34548-w-3628.dbdns.bilibili.co:3628/live_dimension?characterEncoding=utf8',\n" +
                "'userName' = 'live_dimension_61'\n" +
                ")";

        String DDL4 = "create view old_dws as select  TUMBLE_END(event_time, INTERVAL '1' MINUTE)  as window_end, buvid, mid, brand, model, city as city_name, city_level, country, province, jumpfrom, pid, ( case when pid = 1 then 'iphone' when pid = 2 then 'ipad' when pid = 3 then 'android' else '' end ) as platform, version as app_version, event_id, event_category, room_id, up_id, area_id, subarea_id, log_src, count(*) as pv,  min(event_time) as min_time,  max(event_time) as max_time, launch_id from live_tpc_dwd_flow group by TUMBLE(event_time, INTERVAL '1' MINUTE), buvid, mid, brand, model, city, city_level, country, province, jumpfrom, pid, version, event_id, event_category, room_id, up_id, area_id, subarea_id, log_src, launch_id";

        String DDL5 = "create view t1 as select a.window_end, a.buvid, a.mid, a.brand, a.model, a.city_name, a.city_level, a.country, a.province, a.jumpfrom, a.pid, a.platform, a.app_version, a.event_id, a.event_category, a.room_id, a.up_id, a.area_id, a.subarea_id, a.log_src, a.pv, a.min_time, a.max_time, case when a.up_id in (392836434, 50333369, 50329118, 50329220) then 1 when a.subarea_id = 371 then 0 when a.area_id in (3, 6, 2) then 2 when a.area_id = 5 then 3 when a.subarea_id in (21, 145, 207) then 4 else 5 end area_type, b.resource_name as resource_name_click, a.launch_id FROM old_dws a left join resource_dim FOR SYSTEM_TIME AS OF now() AS b on a.event_id = b.event_click";

        String DDL6 = "create view t2 as select a.window_end, a.buvid, a.mid, a.brand, a.model, a.city_name, a.city_level, a.country, a.province, a.jumpfrom, a.pid, a.platform, a.app_version, a.event_id, a.event_category, a.room_id, a.up_id, a.area_id, a.subarea_id, a.log_src, a.pv, a.min_time, a.max_time, a.area_type, a.resource_name_click, c.resource_name resource_name_show, a.launch_id from t1 a left join resource_dim FOR SYSTEM_TIME AS OF now() AS c on a.event_id = c.event_show";

        String DDL7 = "create view t3 as select a.window_end, a.buvid, a.mid, a.brand, a.model, a.city_name, a.city_level, a.country, a.province, a.jumpfrom, a.pid, a.platform, a.app_version, a.event_id, a.event_category, a.room_id, a.up_id, a.area_id, a.subarea_id, a.log_src, a.pv, a.min_time, a.max_time, a.area_type, a.resource_name_click, a.resource_name_show, c.jumpfrom_division, c.jumpfrom_category, c.jumpfrom_class, a.launch_id from t2 a left join jumpfrom_dim FOR SYSTEM_TIME AS OF now() AS c on a.jumpfrom = c.jumpfrom_id";

        String insert = "insert into live_tpc_dws_flow select a.window_end, a.buvid, a.mid, a.brand, a.model, a.city_name, a.city_level, a.country, a.province, a.jumpfrom, a.pid, a.platform, a.app_version, a.event_id, a.room_id, a.up_id, a.area_id, a.subarea_id, a.log_src, a.pv, a.min_time, a.max_time, a.area_type, a.resource_name_click, a.resource_name_show, a.jumpfrom_division, a.jumpfrom_category, a.jumpfrom_class, a.launch_id, a.event_category from t3 a";


        bbTableEnv.executeSql(DDL1);
        bbTableEnv.executeSql(DDL2);
        bbTableEnv.executeSql(DDL3);
        bbTableEnv.executeSql(DDL4);
        bbTableEnv.executeSql(DDL5);
        bbTableEnv.executeSql(DDL6);
        bbTableEnv.executeSql(DDL7);
        bbTableEnv.executeSql(DDL8);
        //bbTableEnv.executeSql(insert);


        List<Operation> operations = bbTableEnv.getParser().parse(insert);
        String ex = bbTableEnv.getPlanner().explain(operations);

        System.out.println("------" + ex);


        //String tt = tEnv.explainSql(insert, CHANGELOG_MODE);
        //System.out.println("=====" + tt);
    }

    @Test
    public void testSqlDelete() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        env.setParallelism(1);

        TableEnvironmentImpl bbTableEnv = TableEnvironmentImpl.create(EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build());

        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
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

        String sql = "CREATE TABLE wxm_test (`userss` BIGINT, `product` BIGINT, `amount` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
        tEnv.executeSql(sql);


        tEnv.executeSql("CREATE TABLE mysql_test_table (\n" +
                "  -- declare the schema of the table\n" +
                "  `name` BIGINT,\n" +
                "  `deptId` BIGINT,\n" +
                "  `salary` BIGINT,\n" +
                " primary key (`name`,`deptId`) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector' = 'bsql-mysql',\n" +
                "'url'='jdbc:mysql://127.0.0.1:3306/wxm_test?characterEncoding=utf8&useSSL=false'," +
                "'userName'='root'," +
                "'password'='123456'," +
                "'batchMaxTimeout' = '3000'," +
                "'parallelism' = '1'," +
                "'tableName'='test_table'" +
                ")");


        Table table = tEnv.sqlQuery("select cast(ceiling(rand() * 100) as bigint) as aaa,456 as bbb, 789 as ccc,111 as ddd,222 as eee from wxm_test");
        Table table1 = tEnv.sqlQuery("select count(t2.wxm) count_wxm,max(t2.wxm) as max_wxm from (select count(*) as count_num,t1.aaa as wxm from " + table + " as t1 where t1.ccc = 789 group by t1.aaa) t2 group by t2.wxm");

       /* TableResult tableResult =
                tEnv.executeSql("insert into mysql_test_table select max_wxm,123,count_wxm from " + table1);*/

       /* System.out.println(tableResult.getResultKind());

        tEnv.toRetractStream(table, RowData.class);

        env.execute("test");*/


        tEnv.executeSql("CREATE TABLE log_test (\n" +
                "  `hour` BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-log'\n" +
                ")");

        tEnv.executeSql("create function to_unixtime as 'com.bilibili.bsql.mysql.ToUnixtime' language java");
        tEnv.executeSql("create function concat_array as 'com.bilibili.bsql.mysql.ConcatArray' language java");

        //tEnv.explainSql("CREATE TABLE log_test (`hour` BIGINT) WITH ('connector' = 'bsql-log')");

        /*tEnv.executeSql("create function TO_TIMESTAMP as 'com.bilibili.bsql.mysql.ToTimestamp' language java");
        tEnv.executeSql("create function FromUnixtime13 as 'com.bilibili.bsql.mysql.FromUnixtime13' language java");
        tEnv.executeSql("create function to_unixtime as 'com.bilibili.bsql.mysql.ToUnixtime' language java");

        sql = "select HOUR(TO_TIMESTAMP('2020-11-18 11:23:00'))";


        sql = "select cast(TO_TIMESTAMP(to_unixtime('2020-11-18 11:23:00','yyyy-MM-dd HH:mm:ss')) as DATE)";*/

        //sql = "select hour(cast('2020-11-11 13:10:00' as TIMESTAMP(3)))";

        sql = "select hour(cast('2020-11-11 13:10:00' as TIMESTAMP(3))),concat_array(',', collcet(concat_ws('|','aaa','bbb')))";

        //tEnv.executeSql("INSERT INTO log_test select HOUR(TO_TIMESTAMP(cast(to_unixtime('2020-11-18 11:23:00','yyyy-MM-dd HH:mm:ss') as varchar))) as `hour`");
        Table table2 = tEnv.sqlQuery(sql);

        tEnv.explainSql("INSERT INTO log_test select hour(cast('2020-11-11 13:10:00' as TIMESTAMP)) as `hour`");

        tEnv.toRetractStream(table2, Row.class);
        env.execute("");
    }
}
