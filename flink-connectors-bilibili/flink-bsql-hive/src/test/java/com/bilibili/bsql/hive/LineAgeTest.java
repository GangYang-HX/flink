package com.bilibili.bsql.hive;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.LineAgeInfo;
import org.apache.flink.table.api.PhysicalExecutionPlan;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.Operation;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className LineAgeTest.java
 * @description This is the description of LineAgeTest.java
 * @createTime 2020-12-22 12:13:00
 */
public class LineAgeTest {








    @Test
    public void test() throws InterruptedException {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironmentImpl tEnv = TableEnvironmentImpl.create(settings);

		tEnv.executeSql("CREATE TABLE sink (\n" +
			"  label VARCHAR,\n" +
			"  mid VARCHAR,\n" +
			"  avid VARCHAR,\n" +
			"  display_time BIGINT,\n" +
			"  instance VARCHAR,\n" +
			"  extra VARCHAR,\n" +
			"  click_state VARCHAR,\n" +
			"  show_state VARCHAR,\n" +
			"  action_state VARCHAR,\n" +
			"  play_state VARCHAR,\n" +
			"  trackid VARCHAR\n" +
			") WITH (\n" +
			"  'timeField' = 'display_time',\n" +
			"  'parallelism' = '5',\n" +
			"  'format' = 'orc',\n" +
			"  'connector.type' = 'bsql-hive',\n" +
			"  'tableName' = 'ai.tmp_fm_fym'\n" +
			")");

		tEnv.executeSql("CREATE TABLE source (\n" +
			"  label_raw VARCHAR,\n" +
			"  mid VARCHAR,\n" +
			") WITH (\n" +
			"  'parallelism' = '5',\n" +
			"  'bootstrapServers' = '10.70.144.26:9092,10.70.144.28:9092,10.70.144.33:9092,10.70.145.11:9092,10.70.145.13:9092',\n" +
			"  'topic' = 'ai_tpc_feature_merge_wdctr',\n" +
			"  'bsql-delimit.delimiterKey' = '\\u0001',\n" +
			"  'offsetReset' = 'latest',\n" +
			"  'connector' = 'bsql-datagen'\n" +
			")");

		tEnv.executeSql("INSERT INTO\n" +
				"  sink\n" +
				"SELECT\n" +
				"  label_raw,\n" +
				"  mid,\n" +
				"  avid,\n" +
				"  display_time * 1000 AS display_time,\n" +
				"  instance,\n" +
				"  reserved,\n" +
				"  click_state,\n" +
				"  show_state,\n" +
				"  action_state,\n" +
				"  play_state,\n" +
				"  trackid\n" +
				"FROM\n" +
				"  source");


		StatementSet stateSet =tEnv.createStatementSet();


		stateSet.execute("aaa");

		while (true) {
			Thread.sleep(1000L);
		}
    }

    /*private static List<Operation> buildOperations(TableEnvironmentImpl bbTableEnv, String bSql) {
        List<Operation> operations = Lists.newArrayList();
        for (String sql : org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens(bSql, ";")) {
            if (org.apache.commons.lang.StringUtils.isNotBlank(sql)) {
                operations.addAll(bbTableEnv.getPlanner().getParser().parse(sql));
            }
        }
        return operations;
    }*/

    private static List<Operation> buildOperations(TableEnvironmentImpl bbTableEnv, String bSql) {
        List<Operation> operations = Lists.newArrayList();
        for (String sql : org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens(bSql, ";")) {
            if (org.apache.commons.lang.StringUtils.isNotBlank(sql)) {
                if (!sql.toLowerCase().contains("insert ")) {
                    bbTableEnv.executeSql(sql);
                }
                operations.addAll(bbTableEnv.getPlanner().getParser().parse(sql));
            }
        }
        return operations;
    }

    private static List<Operation> buildOperations2(TableEnvironmentImpl bbTableEnv, String bSql) {
        List<Operation> operations = Lists.newArrayList();
        for (String sql : StringUtils.splitByWholeSeparatorPreserveAllTokens(bSql, ";")) {
            if (StringUtils.isNotBlank(sql)) {
                if (!sql.toLowerCase().contains("insert ")) {
                    bbTableEnv.executeSql(sql);
                } else {
                    operations.addAll(bbTableEnv.getPlanner().getParser().parse(sql));
                }
            }
        }
        return operations;
    }









}
