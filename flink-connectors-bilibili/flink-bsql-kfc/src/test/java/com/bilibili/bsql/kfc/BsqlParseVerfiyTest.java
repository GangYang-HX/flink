/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kfc;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Objects;

import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlParseVerfiyTest.java, v 0.1 2020-09-29 18:20
zhouxiaogang Exp $$
 */
public class BsqlParseVerfiyTest {
	@Test
	public void testMultiKeyVarchar() throws Exception {

		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		final int REST_PORT = 8081;
		final String CONFIG_PATH = "/Users/zhouhuidong/Documents/workspace/biliflink2/flink-connectors-bilibili/flink-bsql-kfc/src/test/resources/flink-conf.yaml";
		Configuration conf = new Configuration();
		conf.setInteger(RestOptions.PORT, REST_PORT);
//		InputStream ips = BsqlParseVerfiyTest.class.getResourceAsStream(CONFIG_PATH);
		InputStream ips = new FileInputStream(CONFIG_PATH);
		ParameterTool parameter = ParameterTool.fromPropertiesFile(ips);
		conf.addAll(parameter.getConfiguration());
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
					.inStreamingMode()
					.useBlinkPlanner()
					.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
				"  -- declare the schema of the table\n" +
				"  `userss` as 'aa1,bb',\n" +
				"  `product` STRING,\n" +
				"  `amount` INTEGER\n" +
				") WITH (\n" +
				"  -- declare the external system to connect to\n" +
				"  'connector' = 'bsql-datagen',\n" +
				"  'rows-per-second' = '1'\n" +
				")");

		tEnv.executeSql("CREATE TABLE kfcSide (\n" +
				"  -- declare the schema of the table\n" +
				"  user1 STRING,\n" +
				"  address ARRAY<STRING>,\n" +
				"PRIMARY KEY(user1)  NOT ENFORCED\n"+
				") WITH (\n" +
				"  -- declare the external system to connect to\n" +
				"  'connector' = 'bsql-kfc',\n" +
				"  'delimitKey' = ',',\n" +
				"  'url' = 'list://127.0.0.1:8002',\n" +
				" 'retryTimes' = '1'\n" +
				")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss, r.address[1] as product,a.amount   FROM MyUserTable as a" +
				" left join kfcSide FOR SYSTEM_TIME AS OF now() AS r " +
				" on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute("aaa");
	}

	@Test
	public void testMultiKeyBinary() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
					.inStreamingMode()
					.useBlinkPlanner()
					.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
				"  -- declare the schema of the table\n" +
				"  `userss` as 'aa1,bb',\n" +
				"  `product` STRING,\n" +
				"  `amount` INTEGER\n" +
				") WITH (\n" +
				"  -- declare the external system to connect to\n" +
				"  'connector' = 'bsql-datagen',\n" +
				"  'rows-per-second' = '1'\n" +
				")");

		tEnv.executeSql("CREATE TABLE kfcSide (\n" +
				"  -- declare the schema of the table\n" +
				"  user1 STRING,\n" +
				"  address ARRAY<BYTES>,\n" +
				"PRIMARY KEY(user1)  NOT ENFORCED\n"+
				") WITH (\n" +
				"  -- declare the external system to connect to\n" +
				"  'connector' = 'bsql-kfc',\n" +
				"  'delimitKey' = ',',\n" +
				"  'url' = 'list://127.0.0.1:8002'\n" +
				")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss, DECODE(r.address[2], 'UTF-8') as product,a.amount   FROM MyUserTable as a" +
				" left join kfcSide FOR SYSTEM_TIME AS OF now() AS r " +
				" on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

	@Test
	public void testVarcharArray() throws Exception {

		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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

		tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `userss` as 'aa1',\n" +
			"  `product` STRING,\n" +
			"  `amount` INTEGER\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");

		tEnv.executeSql("CREATE TABLE kfcSide (\n" +
			"  -- declare the schema of the table\n" +
			"  user1 STRING,\n" +
			"  address STRING,\n" +
			"PRIMARY KEY(user1)  NOT ENFORCED\n"+
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-kfc',\n" +
			"  'url' = 'list://127.0.0.1:8002'\n" +
			")");
		// join the async table
		Table result = tEnv.sqlQuery("SELECT a.userss as userss, r.address as product,a.amount   FROM MyUserTable as a" +
			" left join kfcSide FOR SYSTEM_TIME AS OF now() AS r " +
			" on r.user1 = a.userss");

		DataStream a = tEnv.toAppendStream(result, Order.class);
		a.print();

		System.err.println(tEnv.explain(result));
		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

    @Test
    public void testBinaryArray() throws Exception {

        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
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



        tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
                "  -- declare the schema of the table\n" +
                "  `userss` as 'aa1',\n" +
                "  `product` STRING,\n" +
                "  `amount` INTEGER\n" +
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-datagen',\n" +
                "  'rows-per-second' = '1'\n" +
                ")");


        tEnv.executeSql("CREATE TABLE RedisSide (\n" +
                "  -- declare the schema of the table\n" +
                "  user1 STRING,\n" +
                "  address BYTES,\n" +
                "PRIMARY KEY(user1)  NOT ENFORCED\n"+
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-kfc',\n" +
                "  'url' = 'list://127.0.0.1:8002'\n" +
                ")");
        // join the async table
        Table result = tEnv.sqlQuery("SELECT a.userss as userss,  DECODE(r.address, 'UTF-8') as product,a.amount   FROM MyUserTable as a" +
                " left join " +
                "RedisSide FOR SYSTEM_TIME AS OF now() AS r " +
                "on r.user1 = a.userss");

        DataStream a = tEnv.toAppendStream(result, Order.class);
        a.print();

        System.err.println(tEnv.explain(result));
        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        env.execute();
    }



	public static void main(String[] args) throws InterruptedException {
		int port = 8002;
		if (args.length == 1) {
			port = Integer.valueOf(args[0]);
		}

		RpcServerOptions options = new RpcServerOptions();
		options.setReceiveBufferSize(64 * 1024 * 1024);
		options.setSendBufferSize(64 * 1024 * 1024);
		final RpcServer rpcServer = new RpcServer(port, options);
//		rpcServer.registerService(new EchoServiceImpl());
		rpcServer.start();

		// make server keep running
		synchronized (BsqlParseVerfiyTest.class) {
			try {
				BsqlParseVerfiyTest.class.wait();
			} catch (Throwable e) {
			}
		}
	}


	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO.
	 */
	public static class Order {
		public String userss;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(String user, String product, int amount) {
			this.userss = user;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + userss +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}
	}
}
