package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** TvfBatchExample. */
public class TvfBatchExample {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(conf).inBatchMode().build();
        TableEnvironment tabEnv = TableEnvironment.create(settings);
        String source =
                "CREATE TABLE source_(f1 bigint, f2 STRING, f3 bigint, f4 TIMESTAMP_LTZ(9)) WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1',\n"
                        + "    'number-of-rows' = '30',\n"
                        + "    'fields.f1.kind' = 'random',\n"
                        + "    'fields.f1.min' = '1',\n"
                        + "    'fields.f1.max' = '10'\n"
                        + "  )";
        tabEnv.executeSql(source);

        String query =
                "select * from (select cast(window_start as string) as window_start, f1, count(distinct f2) as val from TABLE(CUMULATE(TABLE source_, DESCRIPTOR(f3), INTERVAL '2' SECONDS, INTERVAL '15' SECONDS)) group by window_start,f1)c where f1 = 5";

        tabEnv.executeSql(query).print();
    }
}
