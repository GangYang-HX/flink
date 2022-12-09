package com.bilibili.bsql.databus;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlDataBusTest.java
 * @description This is the description of BsqlDataBusTest.java
 * @createTime 2020-10-22 21:12:00
 */
public class BsqlDataBusTest {

	@Test
	public void dataBusTest() throws Exception {
		String[] args = new String[0];
		final ParameterTool params = ParameterTool.fromArgs(args);
		String planner = params.has("planner") ? params.get("planner") : "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(20);

		/*env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		//  RocksDBStateBackend 只支持异步快照，支持增量快照
		String bathPath = "file:///Users/weiximing/code/gitlab/flinkdemo/";
		String path = bathPath + "checkpoint";
		String rocksPath = bathPath + "rocksdb";
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(path);
		rocksDBStateBackend.setDbStoragePath(rocksPath);
		rocksDBStateBackend.setNumberOfTransferingThreads(1);
		rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		env.setStateBackend((StateBackend) new FsStateBackend(path, true));*/

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

		// 10.217.18.18, port:6205
		String sql = "CREATE TABLE wxm_test " +
				"(`userss` VARCHAR) " +
				"WITH (" +
				"'connector'='bsql-databus'," +
				"'topic'='Xiaoyu-T'," +
				"'groupId'='Xiaoyu-DatacenterBigdataUat-S'," +
				"'appKey'='f87a235e8d559f3c'," +
				"'appSecret'='dc952bc42a8393383551d211f7892699'," +
				"'bsql-delimit.delimiterKey'='|'," +
				"'address'='uat-shylf-databus.bilibili.co:6205'," +
				"'partition'='9'" +
				")";
		tEnv.executeSql(sql);

		String sql1 = "CREATE TABLE wxm_test_gen (`userss` BIGINT, `product` BIGINT, `amount` BIGINT,`amount01` BIGINT) WITH ('connector'='bsql-datagen','rows-per-second'='1')";
		tEnv.executeSql(sql1);

		//Table table2 = tEnv.sqlQuery("select * from wxm_test");

		Table table1 = tEnv.sqlQuery("select userss from wxm_test");

		tEnv.toRetractStream(table1, Row.class).print("== ");

		env.execute("");


	}
}
