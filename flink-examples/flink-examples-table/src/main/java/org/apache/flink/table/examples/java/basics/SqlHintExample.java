package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
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
public class SqlHintExample {
	public static void main(String[] args) throws IOException {


		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
		sEnv.enableCheckpointing(300_000);
		sEnv.setStateBackend(new RocksDBStateBackend("file:///tmp/state"));
		StreamTableEnvironment env = StreamTableEnvironment.create(sEnv);
		Configuration conf = env.getConfig().getConfiguration();

		String source1 =
				"CREATE TABLE bsql_datagen_source1 (\n" +
						"  f1 VARCHAR,\n" +
						"  f2 VARCHAR,\n" +
						"  f3 VARCHAR,\n" +
						"  ts AS PROCTIME()\n" +
						") WITH (\n" +
						"  'connector' = 'bsql-datagen',\n" +
						"  'rows-per-second' = '100'\n" +
						")";
		env.executeSql(source1);
		System.out.println(source1);

		String source2 =
				"CREATE TABLE bsql_datagen_source2 (\n" +
						"  f1 VARCHAR,\n" +
						"  f2 VARCHAR,\n" +
						"  f3 VARCHAR,\n" +
						"  ts AS PROCTIME()\n" +
						") WITH (\n" +
						"  'connector' = 'bsql-datagen',\n" +
						"  'rows-per-second' = '100'\n" +
						")";
		env.executeSql(source2);
		System.out.println(source2);

		String sink =
				"create table sink (f1 VARCHAR, f2 VARCHAR, f3 varchar) with ('connector' = 'blackhole')";
		env.executeSql(sink);
		System.out.println(sink);


		String view = "" +
				"CREATE VIEW LeftLatencyJoin as\n" +
				"SELECT  \n" +
				"  a.f1,\n" +
				"  a.f2,\n" +
				"  b.f3,\n" +
				"  a.ts\n" +
				"FROM \n" +
				"  bsql_datagen_source1 as a\n" +
				"LEFT JOIN \n" +
				"bsql_datagen_source2 b ON a.f2 = b.f2\n" +
				"  AND a.ts > b.ts - INTERVAL '1' MINUTE\n" +
				"  AND a.ts < b.ts + INTERVAL '1' MINUTE";
		System.out.println(view);
		env.executeSql(view);

		String view2 = "" +
				"CREATE VIEW LeftLatencyJoin2 as\n" +
				"SELECT \n" +
				"  a.f1,\n" +
				"  a.f2,\n" +
				"  b.f3,\n" +
				"  a.ts\n" +
				"FROM\n" +
				"  LeftLatencyJoin a\n" +
				"LEFT JOIN /*+ LATENCY_JOIN_OPTIONS('merge'='false','distinct'='false','global'='false','delimiter'=',', 'ttlEnable'='false') */ \n" +
				"bsql_datagen_source2 b LATENCY ON a.f2 = b.f2\n" +
				"  AND a.ts > b.ts - INTERVAL '2' MINUTE\n" +
				"  AND a.ts < b.ts + INTERVAL '2' MINUTE";
		System.out.println(view2);
		env.executeSql(view2);


		String view3 = "" +
				"CREATE VIEW LeftLatencyJoin3 as\n" +
				"SELECT  \n" +
				"  a.f1,\n" +
				"  a.f2,\n" +
				"  b.f3,\n" +
				"  a.ts\n" +
				"FROM \n" +
				"  LeftLatencyJoin2 as a\n" +
				"LEFT JOIN \n" +
				"bsql_datagen_source2 as b ON a.f2 = b.f2\n" +
				"  AND a.ts between b.ts - INTERVAL '3' MINUTE and b.ts\n" +
				"  AND b.ts between a.ts - INTERVAL '3' MINUTE and a.ts";
		System.out.println(view3);
		env.executeSql(view3);

		String view4 = "" +
				"CREATE VIEW LeftLatencyJoin4 as\n" +
				"SELECT \n" +
				"  a.f1,\n" +
				"  a.f2,\n" +
				"  b.f3,\n" +
				"  a.ts\n" +
				"FROM\n" +
				"  LeftLatencyJoin3 a\n" +
				"LEFT JOIN /*+ LATENCY_JOIN_OPTIONS('merge'='false','distinct'='false','global'='false','delimiter'=',', 'ttlEnable'='false') */ \n" +
				"bsql_datagen_source2 b LATENCY ON a.f2 = b.f2\n" +
				"  AND a.ts > b.ts - INTERVAL '4' MINUTE\n" +
				"  AND a.ts < b.ts + INTERVAL '4' MINUTE\n" +
				"LEFT JOIN /*+ LATENCY_JOIN_OPTIONS('merge'='true','distinct'='true','global'='true','delimiter'=',', 'ttlEnable'='true') */ \n" +
				"LeftLatencyJoin2 c LATENCY ON a.f2 = c.f2\n" +
				"  AND a.ts > c.ts - INTERVAL '5' MINUTE\n" +
				"  AND a.ts < c.ts + INTERVAL '5' MINUTE\n";
		System.out.println(view4);
		env.executeSql(view4);


		String insert =
				"INSERT INTO\n" +
						"  sink\n" +
						"SELECT\n" +
						"  f1,\n" +
						"  f2,\n" +
						"  f3\n" +
						"FROM\n" +
						"  LeftLatencyJoin4";


		StatementSet statementSet = env.createStatementSet();
		statementSet.addInsertSql(insert);
		System.out.println(insert);

		System.out.println(statementSet.explain());
		statementSet.execute();
	}
}
