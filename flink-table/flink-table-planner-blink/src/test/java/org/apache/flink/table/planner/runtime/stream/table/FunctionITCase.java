/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for user defined functions in the Table API.
 */
public class FunctionITCase extends StreamingTestBase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, 1L),
			Row.of(2, 2L, 1L),
			Row.of(3, 3L, 1L)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, 2L, 1L),
			Row.of(2, 4L, 1L),
			Row.of(3, 6L, 1L)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(a INT, b BIGINT, c BIGINT) WITH ('connector' = 'COLLECTION')");

		Table table = tEnv().from("TestTable")
			.select(
				$("a"),
				call(new SimpleScalarFunction(), $("a"), $("b")),
				call(new SimpleScalarFunction(), $("a"), $("b"))
					.plus(1)
					.minus(call(new SimpleScalarFunction(), $("a"), $("b")))
			);
		execInsertTableAndWaitResult(table, "TestTable");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testDelayJoin() throws Exception {
		TestCollectionTableFactory.reset();
//		TestCollectionTableFactory.initData(sourceData);


		tEnv().executeSql("CREATE TABLE MyUserTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `userss` BIGINT,\n" +
			"  `userss1111` BIGINT,\n" +
			"  ltime ass proctime()\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");


		tEnv().executeSql("CREATE TABLE RedisSide (\n" +
			"  -- declare the schema of the table\n" +
			"  user1 BIGINT,\n" +
			"  address String,\n" +
			"  `amount` INTEGER,\n" +
			"  rtime as proctime()\n" +

			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'topic' = 'topic_name',\n" +
			"  'offsetReset' = 'latest',\n" +
			"  'bootstrapServers' = 'localhost:9092',\n" +
			"  'parallelism' = '5',\n" +
			"  'delimiterKey' = '\\u0003'   -- declare a format for this system\n" +
			")");

		tEnv().executeSql("CREATE TABLE SinkTable (\n" +
			"  -- declare the schema of the table\n" +
			"  `userss` BIGINT,\n" +
			"  `product` STRING\n" +
//			"  `amount` INTEGER\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'print'\n" +
			")");
		// join the async table
		tEnv().executeSql("insert  into SinkTable SELECT a.userss, r.address as product   FROM MyUserTable as a" +
			" left  join " +
			"RedisSide AS r " +
//			"RedisSide FOR SYSTEM_TIME  AS OF TIMESTAMPADD(HOUR, -1, NOW()) AS r " +
			" global on r.user1 = a.userss  " +
			"and a.ltime between r.rtime - INTERVAL '10' SECOND and r.rtime " +
//			"and r.rtime between a.ltime - INTERVAL '1' HOUR and a.ltime" +
			"");

	}

	@Test
	public void testDelayJoinOnline() throws Exception {
		TestCollectionTableFactory.reset();
//		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE FUNCTION AI_APP_FEED_UNFOLD AS 'org.apache.flink.table.planner.runtime.stream.table.AiAppFeedUnfold' LANGUAGE JAVA");
		tEnv().executeSql("CREATE TABLE AppClick (\n" +
			"  -- declare the schema of the table\n" +
			"ip varchar,\n" +
			"ctime bigint,\n" +
			"api varchar,\n" +
			"buvid varchar,\n" +
			"mid varchar,\n" +
			"client varchar,\n" +
			"itemid varchar,\n" +
			"displayid varchar,\n" +
			"err_code varchar,\n" +
			"from_section varchar,\n" +
			"build varchar,\n" +
			"trackid varchar,\n" +
			"auto_play varchar,\n" +
			"from_spmid varchar,\n" +
			"spmid varchar,\n" +
			"rtime as proctime()\n" +
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");


		tEnv().executeSql("CREATE TABLE AppFeed (\n" +
			"  -- declare the schema of the table\n" +
			"  ip varchar,\n" +
			"ctime varchar,\n" +
			"api varchar,\n" +
			"buvid varchar,\n" +
			"mid varchar,\n" +
			"client varchar,\n" +
			"pagetype varchar,\n" +
			"showlist varchar,\n" +
			"displayid varchar,\n" +
			"is_rec varchar,\n" +
			"build varchar,\n" +
			"return_code varchar,\n" +
			"user_feature varchar,\n" +
			"zoneid varchar,\n" +
			"adresponse varchar,\n" +
			"deviceid varchar,\n" +
			"network varchar,\n" +
			"new_user varchar,\n" +
			"flush varchar,\n" +
			"autoplay_card varchar,\n" +
			"trackid varchar,\n" +
			"device_type varchar,\n" +
			"banner_show_case varchar,\n" +
			"ltime as proctime()\n" +

			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'topic' = 'topic_name',\n" +
			"  'offsetReset' = 'latest',\n" +
			"  'bootstrapServers' = 'localhost:9092',\n" +
			"  'parallelism' = '5',\n" +
			"  'delimiterKey' = '\\u0003'   -- declare a format for this system\n" +
			")");

		tEnv().executeSql("CREATE TABLE AppAiJoin (\n" +
			"  -- declare the schema of the table\n" +

			"  ctime VARCHAR,\n" +
			"mid VARCHAR,\n" +
			"content_type VARCHAR,\n" +
			"content_raw_feature VARCHAR,\n" +
			"mid_raw_feature VARCHAR,\n" +
			"content_id VARCHAR,\n" +
			"label VARCHAR\n" +

			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'print'\n" +
			")");
		// join the async table
		tEnv().executeSql("CREATE VIEW AppClickTransform AS SELECT CONCAT(mid, '-', itemid) as click_key, 1 as label,rtime from AppClick where from_section = '7' and buvid <> '0' and buvid <> '' and mid <> '' and mid <> '0'");
		tEnv().executeSql("CREATE VIEW AppFeedTransform AS select CONCAT(feed.mid, '-', showItem.b) as feed_key, feed.ctime as ctime, feed.mid as mid, '0' as content_type, showItem.a as content_raw_feature, feed.user_feature as mid_raw_feature, showItem.b as content_id, ltime FROM AppFeed as feed, LATERAL TABLE(AI_APP_FEED_UNFOLD(showlist)) AS showItem(a, b) where trim(api) = '/x/feed/index' and return_code = '0' and buvid <> '0' and buvid <> '' and mid <> '' and mid <> '0'");
		tEnv().executeSql("INSERT into AppAiJoin select a.ctime, a.mid, a.content_type, a.content_raw_feature, a.mid_raw_feature, a.content_id, case when b.label = 1 then '1' else '0' end as label from AppFeedTransform a left join AppClickTransform b global on a.feed_key = b.click_key \n" +
			"and a.ltime between b.rtime - INTERVAL '1' HOUR and b.rtime \n" +
			"and b.rtime between a.ltime - INTERVAL '1' HOUR and a.ltime");

	}

	@Test
	public void testJoinWithTableFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of("1,2,3"),
			Row.of("2,3,4"),
			Row.of("3,4,5"),
			Row.of((String) null)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of("1,2,3", new String[]{"1", "2", "3"}),
			Row.of("2,3,4", new String[]{"2", "3", "4"}),
			Row.of("3,4,5", new String[]{"3", "4", "5"})
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING>) WITH ('connector' = 'COLLECTION')");

		TableResult tableResult = tEnv().from("SourceTable")
			.joinLateral(call(new SimpleTableFunction(), $("s")).as("a", "b"))
			.select($("a"), $("b"))
			.executeInsert("SinkTable");
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testLateralJoinWithScalarFunction() throws Exception {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Currently, only table functions can emit rows.");

		TestCollectionTableFactory.reset();
		tEnv().executeSql("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING>) WITH ('connector' = 'COLLECTION')");

		TableResult tableResult = tEnv().from("SourceTable")
			.joinLateral(call(new RowScalarFunction(), $("s")).as("a", "b"))
			.select($("a"), $("b"))
			.executeInsert("SinkTable");
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
	}

	// --------------------------------------------------------------------------------------------
	// Test functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Simple scalar function.
	 */
	public static class SimpleScalarFunction extends ScalarFunction {
		public Long eval(Integer i, Long j) {
			return i + j;
		}
	}

	/**
	 * Scalar function that returns a row.
	 */
	@FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING>>"))
	public static class RowScalarFunction extends ScalarFunction {
		public Row eval(String s) {
			return Row.of(s, s.split(","));
		}
	}

	/**
	 * Table function that returns a row.
	 */
	@FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING>>"))
	public static class SimpleTableFunction extends TableFunction<Row> {
		public void eval(String s) {
			if (s == null) {
				collect(null);
			} else {
				collect(Row.of(s, s.split(",")));
			}
		}
	}

	/**
	 * Simple POJO.
	 */
	public static class Order {
		public Long userss;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(Long user, String product, int amount) {
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
