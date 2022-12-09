package com.bilibili.bsql.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import scala.concurrent.duration.Duration;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

/** Test for bilibili connector. */
public class LancerSqlTest {
    @Test
    public void retryKafkaTest() throws Exception {
        Duration duration = Duration.create("30 s");
        System.err.println(duration);

        // set up execution environment
        Configuration configuration = new Configuration();
        configuration.setString(String.valueOf(TABLE_EXEC_SOURCE_IDLE_TIMEOUT), "30 sec");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tEnv;
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql(
                "CREATE TABLE source_test (\n"
                        + "  id string,\n"
                        + "  topic_name string metadata from 'topic', \n"
                        + " `partition_id` INT METADATA FROM 'partition',\n"
                        + "  `offset` BIGINT METADATA,\n"
                        + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                        + " `timestamp-type` string METADATA,\n"
                        + "  `headers` MAP<STRING, BYTES> METADATA\n"
                        + ") WITH(\n"
                        + "  'connector' = 'bsql-kafka',\n"
                        + "  'topic' = 'meimeizitest1',\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'value.format' = 'raw',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'properties.group.id' = 'test'"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_test (\n"
                        + "  id string,\n"
                        + "  `headers` MAP<STRING, BYTES> METADATA\n"
                        + ") WITH (\n"
                        + "  'connector' = 'bsql-kafka',\n"
                        + "  'topic' = 'meimeizitest2',\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'value.format' = 'raw'"
                        + ")");

        //        tEnv.executeSql("CREATE TABLE sink_test (\n" +
        //                "  id BIGINT,\n" +
        //                "  topic_name string ,\n" +
        //                "  `offset` BIGINT ,\n" +
        //                "  `timestamp` TIMESTAMP(3) METADATA,\n" +
        //                " `timestamp-type` string,\n" +
        //                "  `headers` MAP<STRING, BYTES> METADATA\n" +
        //                ") WITH (\n" +
        //                "  'connector' = 'kafka',\n" +
        //                "  'topic' = 'meimeizitest2',\n" +
        //                "  'properties.bootstrap.servers' =
        // '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
        //                "  'value.format' = 'json'" +
        //                ")");
        //        tEnv.executeSql("CREATE TABLE sink_test (\n" +
        //                "  id BIGINT,\n" +
        //                "  topic_name string,\n" +
        //                "  `timestamp` TIMESTAMP(3) METADATA\n" +
        //                ") WITH (\n" +
        //                "  'connector' = 'kafka',\n" +
        //                "  'topic' = 'meimeizitest2',\n" +
        //                "  'properties.bootstrap.servers' =
        // '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
        //                "  'value.format' = 'json'" +
        //                ")");

        /*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
        env.execute();*/

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql(
                "INSERT INTO sink_test "
                        + "SELECT "
                        + "a.id,"
                        + "a.`headers`"
                        + " FROM source_test as a");

        //        stateSet.addInsertSql("INSERT INTO sink_test " +
        //                "SELECT " +
        //                "a.id," +
        //                "a.topic_name," +
        //                "a.`offset`," +
        //                "a.`timestamp`," +
        //                "a.`timestamp-type`," +
        //                "a.`headers`" +
        //                " FROM source_test as a");
        //        stateSet.addInsertSql("INSERT INTO sink_test " +
        //                "SELECT " +
        //                "a.id," +
        //                "a.topic_name," +
        //                "a.`timestamp`" +
        //                " FROM source_test as a");
        //        stateSet.execute("aaa");
        stateSet.execute();

        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void bootstrapAutoFillingTest() throws Exception {
        Duration duration = Duration.create("30 s");
        System.err.println(duration);

        // set up execution environment
        Configuration configuration = new Configuration();
        configuration.setString(String.valueOf(TABLE_EXEC_SOURCE_IDLE_TIMEOUT), "30 sec");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tEnv;
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, settings);

        String createSource =
                "CREATE TABLE source (\n"
                        + "\turn string,\n"
                        + "\taspect string,\n"
                        + "\tproperties string,\n"
                        + "    action int\n"
                        + ") WITH (\n"
                        + "\t'connector' = 'kafka',\n"
                        + "\t'topic' = 'r_bdp_platform.tpc_keeper_entity',\n"
                        //						+ "\t'properties.bootstrap.servers' =
                        // '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
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
                        + "\t'topic' = 'r_bdp_platform.tpc_keeper_entity',\n"
                        //						+ "\t'properties.bootstrap.servers' = '127.0.0.1:9092',\n"
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
}
