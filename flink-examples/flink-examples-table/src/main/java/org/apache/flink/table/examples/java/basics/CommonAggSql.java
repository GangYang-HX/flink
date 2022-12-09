package org.apache.flink.table.examples.java.basics;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * @author zhangyang
 * @Date:2022/4/22
 * @Time:16:06
 */
public class CommonAggSql {
    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        sEnv.enableCheckpointing(10_000);
        sEnv.setStateBackend(new RocksDBStateBackend("file:///tmp/state"));
        StreamTableEnvironment env = StreamTableEnvironment.create(sEnv);

        String source =
                "create table source_ (\n"
                        + "  buvid varchar,\n"
                        + "  ts as proctime()\n"
                        + ") with (\n"
                        + "  'connector' = 'bsql-datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'number-of-rows' = '1000'\n"
                        + ")";
        env.executeSql(source);

        String sink =
                "create table sink_ (\n"
                        + "  buvid varchar,\n"
                        + "  pv bigint\n"
                        + ") with ('connector' = 'blackhole')";
        env.executeSql(sink);

        String insert =
                "insert into\n"
                        + "  sink_\n"
                        + "select\n"
                        + "  buvid,\n"
                        + "  count(1) as pv\n"
                        + "from\n"
                        + " source_\n"
                        + "group by\n"
                        + "  buvid";
        StatementSet statementSet = env.createStatementSet();
        statementSet.addInsertSql(insert);
        statementSet.execute();
    }
}
