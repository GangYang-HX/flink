/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka;

import java.util.Objects;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.junit.Test;

import com.bilibili.bsql.util.kfkClientUtil;

import static com.bilibili.bsql.common.keys.TableInfoKeys.SABER_JOB_ID;
import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;


/**
 * @author zhouxiaogang
 * @version $Id: BsqlParseVerfiyTest.java, v 0.1 2020-09-29 18:20
 * zhouxiaogang Exp $$
 */
public class KafkaSourceTest {

    @Test
    public void kafkaSource() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        Configuration flinkConfig = new Configuration();
        flinkConfig.set(SABER_JOB_ID, "for test");

        TableConfig customTableConfig = new TableConfig();
        customTableConfig.addConfiguration(flinkConfig);
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useOldPlanner()
                    .build();
            tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
        } else {
            System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
                    "where planner (it is either flink or blink, and the default is blink) indicates whether the " +
                    "example uses flink planner or blink planner.");
            return;
        }

        env.setStreamTimeCharacteristic(EventTime);
        env.getConfig().setParallelism(10);

        tEnv.executeSql("create function MY_TO_TIMESTAMP as 'com.bilibili.bsql.kafka.ToTimestamp' language java");
        tEnv.executeSql("create function MY_TO_UNIXTIME as 'com.bilibili.bsql.kafka.ToUnixtime' language java");

        tEnv.executeSql("CREATE TABLE my_kafka_test (\n" +
                "  `name` VARCHAR,\n" +
                "  `num` int,\n" +
                "  `xtime` AS TO_TIMESTAMP(name ),\n" +
			"WATERMARK  FOR xtime as xtime- INTERVAL '0.1' SECOND \n"+
                ") WITH (\n" +
                "  'connector' = 'bsql-kafka10',\n" +
                "  'topic' = 'zzj_test',\n" +
                "  'defOffsetReset' = 'latest',\n" +
                "  'offsetReset' = '1647335924565',\n" +
                "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
                "  'bsql-delimit.delimiterKey' = '|'   -- declare a format for this system\n" +
                ")");
        // union the two tables

        String sql = "SELECT name,num,xtime" +
                " FROM my_kafka_test";

        //sql = "SELECT hour(cast(xtime as TIMESTAMP(3))),MY_TO_TIMESTAMP(MY_TO_UNIXTIME(xtime,'yyyy-MM-dd HH:mm:ss')) from my_kafka_test";

        //sql = "SELECT hour(cast(xtime as TIMESTAMP(3))) from my_kafka_test";
        Table result = tEnv.sqlQuery(sql);

        DataStream a = tEnv.toAppendStream(result, Row.class);
        a.print();
        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        env.execute();
    }

    @Test
    public void testOpenProducer() throws Exception {
        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        Configuration flinkConfig = new Configuration();
        flinkConfig.set(SABER_JOB_ID, "for test");

        TableConfig customTableConfig = new TableConfig();
        customTableConfig.addConfiguration(flinkConfig);
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useOldPlanner()
                    .build();
            tEnv = StreamTableEnvironmentImpl.create(env, settings, customTableConfig);
        } else {
            System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
                    "where planner (it is either flink or blink, and the default is blink) indicates whether the " +
                    "example uses flink planner or blink planner.");
            return;
        }

        env.setStreamTimeCharacteristic(EventTime);
		tEnv.executeSql(
			"CREATE TABLE ubt_source ("+
			"event_id varchar,\n" +
				"  app_info ROW<app_id int,platform int,buvid varchar,chid varchar,brand varchar,device_id varchar,model varchar,osver varchar,fts bigint,buvid_shared varchar,uid int,api_level int,abi varchar,bilifp varchar,session_id varchar>,\n" +
				"  runtime_info ROW<network varchar,oid varchar,longitude double,latitude double,version varchar,version_code varchar,logver varchar,abtest varchar,ff_version varchar>,\n" +
				"  mid varchar,\n" +
				"  ctime bigint,\n" +
				"  log_id varchar,\n" +
				"  retry_send_count int,\n" +
				"  sn bigint,\n" +
				"  event_category varchar,\n" +
				"  app_page_view_info ROW<event_id_from varchar,load_type int,duration bigint,pvstart bigint,pvend bigint>,\n" +
				"  extended_fields MAP<varchar,varchar>,\n" +
				"  page_type int,\n" +
				"  sn_gen_time bigint,\n" +
				"  upload_time bigint,\n" +
				"  app_player_info ROW<play_from_spmid varchar,season_id varchar,type int,sub_type int,ep_id varchar,progress varchar,avid varchar,cid varchar,network_type int,danmaku int,`status` int,play_method int,play_type int,player_session_id varchar,speed varchar,player_clarity varchar,is_autoplay int,video_format int>,\n" +
				"   headers MAP<varchar,bytes> METADATA" +
				") WITH (\n" +
				"  -- declare the external system to connect to\n" +
				"  'connector' = 'bsql-kafka10',\n" +
				"  'topic' = 'zzj_test',\n" +
				"  'offsetReset' = 'latest',\n" +
//			"  'bootstrapServers' = '172.22.33.99:9092,172.22.33.97:9092',\n" +
				"  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
				"  'parallelism' = '5',\n" +
				"  'bsql-delimit.delimiterKey' = '\\u0001'   -- declare a format for this system\n" +
				")"
		);

//        tEnv.executeSql(
//        	"CREATE TABLE MyUserTable (\n" +
//                "  -- declare the schema of the table\n" +
//                "  `name` STRING,\n" +
//                "  `num` INT,\n" +
//                "  num1 as num * 2," +
//                "  `xtime` TIMESTAMP,\n" +
//                " proctime as proctime()\n" +
////                " WATERMARK FOR xtime as xtime - INTERVAL '0.2' SECOND\n" +
//                ") WITH (\n" +
//                "  -- declare the external system to connect to\n" +
//                "  'connector' = 'bsql-kafka10',\n" +
//                "  'topic' = 'zzj_test',\n" +
//                "  'offsetReset' = 'latest',\n" +
////			"  'bootstrapServers' = '172.22.33.99:9092,172.22.33.97:9092',\n" +
//                "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
//                "  'parallelism' = '5',\n" +
//                "  'bsql-delimit.delimiterKey' = '\\u0001'   -- declare a format for this system\n" +
//                ")");
        // union the two tables
//        Table result = tEnv.sqlQuery("SELECT name,num1 as num,proctime as xtime FROM MyUserTable");
        Table result = tEnv.sqlQuery("SELECT log_id FROM ubt_source");

//        DataStream a = tEnv.toAppendStream(result, kfkClientUtil.MyTable.class);
//        a.print();
        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        env.execute();
    }

//	@Test
//	public void testForKafkaSink() throws Exception {
//1
//	}

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}
