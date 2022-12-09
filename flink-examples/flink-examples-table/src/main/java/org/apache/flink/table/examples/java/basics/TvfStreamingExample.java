package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** TvfStreamingExample. */
public class TvfStreamingExample {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(conf).inStreamingMode().build();
        TableEnvironment tabEnv = TableEnvironment.create(settings);
        String source =
                "CREATE TABLE source_(f1 bigint, f2 STRING, f3 bigint, f4 TIMESTAMP_LTZ(3), WATERMARK FOR f4 AS f4 - INTERVAL '0.001' SECOND) WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1',\n"
                        + "    'number-of-rows' = '60',\n"
                        + "    'fields.f1.kind' = 'random',\n"
                        + "    'fields.f1.min' = '1',\n"
                        + "    'fields.f1.max' = '10'\n"
                        + "  )";
        tabEnv.executeSql(source);

        String createView =
                "create view view_ as select window_start, f1, count(distinct f2) from TABLE(HOP(TABLE source_, DESCRIPTOR(f4), INTERVAL '5' SECONDS, INTERVAL '10' SECONDS)) group by window_start,f1";

        tabEnv.executeSql(createView);

        String query = "explain select * from view_ where f1 = 5";

        tabEnv.executeSql(query).print();
    }
}
