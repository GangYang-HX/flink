package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import java.io.IOException;

/** CommonSqlExample. */
public class CommonSqlExample {
    public static void main(String[] args) throws IOException {

        KerberosUtil.init();

        Configuration conf = new Configuration();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(conf).inStreamingMode().build();
        TableEnvironmentImpl env = (TableEnvironmentImpl) TableEnvironment.create(settings);

        env.executeSql(
                "CREATE TABLE kafka_data_source(\n"
                        + "  f1 varchar,\n"
                        + "  f2 varchar,\n"
                        + "  f3 varchar,\n"
                        + "  ts timestamp(3),\n"
                        + "  WATERMARK FOR ts AS ts - INTERVAL '2' MINUTES\n"
                        + ") WITH(\n"
                        + "  'topic' = 'kafka_data_source',\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'value.format' = 'csv',\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'value.csv.field-delimiter' = '|',\n"
                        + "  'value.csv.ignore-parse-errors' = 'true',\n"
                        + "  'scan.startup.mode' = 'latest-offset'\n"
                        + ")");

        env.executeSql(
                "CREATE TABLE kafka_data_sink(\n"
                        + "  f1 varchar,\n"
                        + "  f2 varchar,\n"
                        + "  f3 varchar \n"
                        + ") WITH(\n"
                        + "  'topic' = 'kafka_data_sink',\n"
                        + "  'properties.bootstrap.servers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n"
                        + "  'value.format' = 'csv',\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'value.csv.field-delimiter' = '|',\n"
                        + "  'value.csv.ignore-parse-errors' = 'true',\n"
                        + "  'scan.startup.mode' = 'latest-offset'\n"
                        + ")");

        String explainSql =
                "explain insert into kafka_data_sink select f1,f2,f3 from kafka_data_source";
        env.executeSql(explainSql).print();

        //        StatementSet set = env.createStatementSet();
        //        set.addInsertSql("explain insert into kafka_data_sink select f1,f2,f3 from
        // kafka_data_source");
        //        set.execute();
    }
}
