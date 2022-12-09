package com.bilibili.bsql.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Test;

/** @Author: JinZhengyu @Date: 2022/5/20 下午8:43 */
public class MyselfTest {

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @Before
    public void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setString(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID.key(), "abcdefg");
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void delimitTestWithHeader() throws Exception {
        String createSource =
                "CREATE TABLE source ("
                        + "  `body` bytes,"
                        + "  `timestamp` TIMESTAMP(3) METADATA,"
                        + " `timestamp-type` string METADATA,"
                        + "  `headers` MAP<varchar,bytes> METADATA,"
                        + "WATERMARK  FOR `timestamp` as `timestamp` - INTERVAL '0.1' SECOND "
                        + ") WITH ("
                        + "'connector' = 'bsql-kafka',"
                        + "'format' = 'bsql-delimit',"
                        + "'topic' = 'source_topic_leader_1',"
                        //                        + "'partition.discovery.interval.ms' = '30000',"
                        //                        + "'partition.discovery.timeout.ms' = '30',"
                        //                        + "'partition.discovery.retries' = '2',"
                        + "'properties.bootstrap.servers' = '127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094',"
                        + "'properties.group.id' = 'testGroup',"
                        + "'scan.startup.mode' = 'earliest-offset',"
                        + "'bsql-delimit.delimiterKey' = ',',"
                        + "'bsql-delimit.format' = 'bytes'"
                        + ")";
        tEnv.executeSql(createSource);
        String createSink =
                "CREATE TABLE sink ("
                        + "  `value` string,"
                        + "  `tableName` String,"
                        + "  `dbName` String,"
                        + "  `timestamp` TIMESTAMP(3) METADATA,"
                        + "  `headers` MAP<STRING, bytes> METADATA"
                        + ") WITH ("
                        + "'connector' = 'bsql-kafka',"
                        + "'format' = 'csv',"
                        + "'topic' = 'sink_topic',"
                        + "'properties.bootstrap.servers' = '127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094'"
                        + ")";
        tEnv.executeSql(createSink);
        String insertSQL =
                "INSERT INTO sink "
                        + "SELECT "
                        + "cast(a.`body` as string) as body,"
                        + "cast(a.`headers`['tabName'] as string) as tableName,"
                        + "cast(a.`headers`['dbName'] as string) as dbName,"
                        + "a.`timestamp`,"
                        + "a.`headers`"
                        + " FROM source as a";

        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(insertSQL);
        statementSet.execute("delimitTest");
        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void myTest() throws Exception {
        String topic = "source_topic";
        String bootstraps = "127.0.0.1:9092";
        final String source =
                "CREATE TABLE t_student (\n"
                        + "\tid INT,\n"
                        + "\tname STRING\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "\t'topic' = 'source_topic',\n"
                        + "\t'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "\t'properties.group.id' = 'flink-cdc-mysql-kafka',\n"
                        + "\t'scan.startup.mode' = 'earliest-offset',\n"
                        + "\t'format' = 'json'\n"
                        + ")\n";

        final String createTable =
                String.format(
                        "create table kafka (\n"
                                + "  `computed-price` as price + 1.0,\n"
                                + "  price decimal(38, 18),\n"
                                + "  currency string,\n"
                                + "  log_date date,\n"
                                + "  log_time time(3),\n"
                                + "  log_ts timestamp(3),\n"
                                + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                                + "  watermark for ts as ts\n"
                                + ") with (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'scan.startup.mode' = 'latest-offset',\n"
                                + "  'format' = 'json'"
                                + ")",
                        topic, bootstraps);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "SELECT CAST(price AS DECIMAL(10, 2)), currency, "
                        + " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n"
                        + "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n"
                        + "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n"
                        + "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n"
                        + "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n"
                        + "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n"
                        + "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n"
                        + "  AS orders (price, currency, d, t, ts)";
        tEnv.executeSql(initialValues);

        String query =
                "SELECT\n"
                        + "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n"
                        + "  CAST(MAX(log_date) AS VARCHAR),\n"
                        + "  CAST(MAX(log_time) AS VARCHAR),\n"
                        + "  CAST(MAX(ts) AS VARCHAR),\n"
                        + "  COUNT(*),\n"
                        + "  CAST(MAX(price) AS DECIMAL(10, 2))\n"
                        + "FROM kafka\n"
                        + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        Table result = tEnv.sqlQuery(query);
        tEnv.toDataStream(result).print();

        env.execute();
    }

    @Test
    public void consumeAndPrint() throws Exception {
        String sql =
                "CREATE TABLE source (\n"
                        + "\turn string,\n"
                        + "\taspect string,\n"
                        + "\tproperties string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "\t'topic' = 'r_bdp_platform.tpc_keeper_entity',\n"
                        + "\t'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "\t'scan.startup.mode' = 'earliest-offset',\n"
                        + "\t'format' = 'json'\n"
                        + ")";
        tEnv.executeSql(sql);
        String query = "select urn,properties,action ,aspect from source";
        Table result = tEnv.sqlQuery(query);
        tEnv.toDataStream(result).print();
        env.execute();
    }

    @Test
    public void consumeAndSink() throws Exception {
        String createSource =
                "CREATE TABLE source (\n"
                        + "\turn string,\n"
                        + "\taspect string,\n"
                        + "\tproperties string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "\t'topic' = 'r_bdp_platform.tpc_keeper_entity',\n"
                        + "\t'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "\t'scan.startup.mode' = 'earliest-offset',\n"
                        + "\t'format' = 'json'\n"
                        + ")";
        tEnv.executeSql(createSource);
        String createSink =
                "CREATE TABLE sink (\n"
                        + "\turn string,\n"
                        + "\taspect string,\n"
                        + "    properties string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "\t'topic' = 'sink_topic',\n"
                        + "\t'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "\t'scan.startup.mode' = 'latest-offset',\n"
                        + "\t'format' = 'json'\n"
                        + ")";
        tEnv.executeSql(createSink);

        String insertSQL =
                "insert into sink(urn,aspect,properties,action) select urn,aspect,properties,action from source";
        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(insertSQL); // 这里进入CatalogSourceTable.toRel()
        statementSet.execute();
        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void retryKafkaTest() throws Exception {
        String createSource =
                "CREATE TABLE source (\n"
                        + "\turn string,\n"
                        + "\taspect string,\n"
                        + "\tproperties string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'bsql-kafka',\n"
                        + "\t'topic' = 'r_bdp_platform.tpc_keeper_entity',\n"
                        + "\t'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "\t'scan.startup.mode' = 'earliest-offset',\n"
                        + "\t'format' = 'json'\n"
                        + ")";
        tEnv.executeSql(createSource);
        String createSink =
                "CREATE TABLE sink (\n"
                        + "\turn string,\n"
                        + "\taspect string,\n"
                        + "    properties string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'bsql-kafka',\n"
                        + "\t'topic' = 'sink_topic',\n"
                        + "\t'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "\t'format' = 'json'\n"
                        + ")";
        tEnv.executeSql(createSink);

        String insertSQL =
                "insert into sink(urn,aspect,properties,action) select urn,aspect,properties,action from source";
        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(insertSQL); // 这里进入CatalogSourceTable.toRel()
        statementSet.execute();
        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void csvTest() throws Exception {
        String createSource =
                "CREATE TABLE source (\n"
                        + "\tid string,\n"
                        + "\tname string,\n"
                        + "\tnickname string,\n"
                        + "  action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "'format' = 'csv',"
                        + "\t'topic' = 'source_topic',\n"
                        + "'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "'properties.group.id' = 'testGroup',\n"
                        + "'scan.startup.mode' = 'latest-offset',"
                        + "'csv.field-delimiter' = ',',"
                        + "'csv.ignore-parse-errors' = 'true'"
                        + ")";
        tEnv.executeSql(createSource);
        String createSink =
                "CREATE TABLE sink (\n"
                        + "\tid string,\n"
                        + "\tname string,\n"
                        + "    nickname string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "'format' = 'csv',"
                        + "\t'topic' = 'sink_topic',\n"
                        + "\t'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + " 'csv.field-delimiter' = ',',"
                        + "'csv.ignore-parse-errors' = 'true'"
                        + ")";
        tEnv.executeSql(createSink);

        String insertSQL =
                "INSERT INTO sink "
                        + "SELECT "
                        + "a.id,"
                        + "a.name,"
                        + "a.nickname,"
                        + "a.action"
                        + " FROM source as a";
        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(insertSQL); // 这里进入CatalogSourceTable.toRel()
        statementSet.execute("csvTest");
        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void delimitTest() throws Exception {
        String createSource =
                "CREATE TABLE source (\n"
                        + "\tid string,\n"
                        + "\tname int,\n"
                        + "\tnickname string,\n"
                        + "  action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'bsql-kafka',\n"
                        + "'format' = 'bsql-delimit',"
                        + "\t'topic' = 'source_topic',\n"
                        + "'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "'properties.group.id' = 'testGroup',\n"
                        //                        + "'scan.startup.mode' = 'latest-offset',"
                        + "'bsql-delimit.delimiterKey' = ',',"
                        + "'bsql-delimit.raw' = 'true'"
                        //                +  ",'bsql-delimit.noDefaultValue' = 'true'"
                        //                +  ",'bsql-delimit.format' = 'bytes'"
                        + ",'bsql-delimit.useLancerFormat' = 'true'"
                        + ",'bsql-delimit.traceId' = '1234'"
                        + ",'bsql-delimit.sinkDest' = 'sss'"
                        + ")";
        tEnv.executeSql(createSource);
        String createSink =
                "CREATE TABLE sink (\n"
                        + "\tid string,\n"
                        + "\tname int,\n"
                        + "    nickname string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'bsql-kafka',\n"
                        + "'format' = 'csv',"
                        + "\t'topic' = 'sink_topic',\n"
                        + "\t'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
                        + "'csv.field-delimiter' = ','"
                        + ")";
        tEnv.executeSql(createSink);

        String insertSQL =
                "INSERT INTO sink "
                        + "SELECT "
                        + "a.id,"
                        + "a.name,"
                        + "a.nickname,"
                        + "a.action"
                        + " FROM source as a";
        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(insertSQL); // 这里进入CatalogSourceTable.toRel()
        statementSet.execute("delimitTest");
        while (true) {
            Thread.sleep(1000L);
        }
    }
}
