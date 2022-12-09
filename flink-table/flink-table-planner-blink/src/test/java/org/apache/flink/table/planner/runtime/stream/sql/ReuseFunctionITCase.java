package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Dove
 * @Date 2022/2/18 10:45 上午
 */
public class ReuseFunctionITCase extends StreamingTestBase {

	@Test
	public void testReusePrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "11-", "-"),
			Row.of(2, "22--", "--"),
			Row.of(3, "33---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("PrimitiveScalarFunctionReuseAcc", PrimitiveScalarFunctionReuseAcc.class);
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, PrimitiveScalarFunctionReuseAcc(i, b, s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where m is not null";
		execInsertSqlAndWaitResult(sql);

		// 数据源条数 和 执行ScalarFunction的次数一致
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
	}

	@Test
	public void testReuseMultiPrimitiveScalarFunctionWithOrExpr() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "13-", "-"),
			Row.of(2, "26--", "--"),
			Row.of(3, "310---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("oneUdf", PrimitiveScalarFunctionReuseAccOne.class);
		tEnv().createTemporarySystemFunction("twoUdf", PrimitiveScalarFunctionReuseAcc.class);
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, twoUdf(i, oneUdf(i,b,s), s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		// 嵌套UDF均被调用一次，前两条数据在Projection中eval，第三条数据在Filter中eval，Projection的value复用Filter结果
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAccOne.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
		PrimitiveScalarFunctionReuseAccOne.evalNum = 0;
	}

	@Test
	public void testReuseMultiPrimitiveScalarFunctionWithOrExprOld() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "13-", "-"),
			Row.of(2, "26--", "--"),
			Row.of(3, "310---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("oneUdf", new PrimitiveScalarFunctionReuseAccOne());
		tEnv().registerFunction("twoUdf", new PrimitiveScalarFunctionReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, twoUdf(i, oneUdf(i,b,s), s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		// 嵌套UDF均被调用一次，前两条数据在Projection中eval，第三条数据在Filter中eval，Projection的value复用Filter结果
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAccOne.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
		PrimitiveScalarFunctionReuseAccOne.evalNum = 0;
	}

	@Test
	public void testReuseSelfMultiPrimitiveScalarFunctionWithOrExpr() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "1111-", "-"),
			Row.of(2, "2222--", "--"),
			Row.of(3, "3434---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("oneUdf", new PrimitiveScalarFunctionReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, oneUdf(i, b, oneUdf(i,b,s)) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		// 嵌套UDF均被调用两次，前两条数据在Projection中eval，第三条数据在Filter中eval，Projection的value复用Filter结果
		assertThat(sourceData.size() * 2, equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
	}

	@Test
	public void testReuseSelfMultiPrimitiveScalarFunctionWithOrExprOld() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "1111-", "-"),
			Row.of(2, "2222--", "--"),
			Row.of(3, "3434---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("oneUdf", new PrimitiveScalarFunctionReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, oneUdf(i, b, oneUdf(i,b,s)) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		// 嵌套UDF均被调用两次，前两条数据在Projection中eval，第三条数据在Filter中eval，Projection的value复用Filter结果
		assertThat(sourceData.size() * 2, equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
	}

	@Test
	public void testNoReuseMultiPrimitiveScalarFunctionWithOrExpr() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "13-", "-"),
			Row.of(2, "26--", "--"),
			Row.of(3, "310---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("oneUdf", PrimitiveScalarFunctionReuseAccOne.class);
		tEnv().createTemporarySystemFunction("twoUdf", PrimitiveScalarFunctionNoReuseAcc.class);
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, twoUdf(i, oneUdf(i,b,s), s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		// 前两条数据twoUdf都仅在Projection中被调用一次，第三条数据twoUdf在Filter和Projection中均被调用一次，所以是2+2==sourceData.size()+1
		assertThat(sourceData.size() + 1, equalTo(PrimitiveScalarFunctionNoReuseAcc.evalNum));
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAccOne.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionNoReuseAcc.evalNum = 0;
		PrimitiveScalarFunctionReuseAccOne.evalNum = 0;
	}

	@Test
	public void testReuseMultiPrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "13-", "-"),
			Row.of(2, "26--", "--"),
			Row.of(3, "39---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("oneUdf", PrimitiveScalarFunctionReuseAccOne.class);
		tEnv().createTemporarySystemFunction("twoUdf", PrimitiveScalarFunctionReuseAcc.class);
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, twoUdf(i, oneUdf(i,b,s), s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where m is not null";
		execInsertSqlAndWaitResult(sql);

		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAccOne.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
		PrimitiveScalarFunctionReuseAccOne.evalNum = 0;
	}

	@Test
	public void testReuseMultiPrimitiveScalarFunctionOld() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "13-", "-"),
			Row.of(2, "26--", "--"),
			Row.of(3, "39---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("oneUdf", new PrimitiveScalarFunctionReuseAccOne());
		tEnv().registerFunction("twoUdf", new PrimitiveScalarFunctionReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, twoUdf(i, oneUdf(i,b,s), s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where m is not null";
		execInsertSqlAndWaitResult(sql);

		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAccOne.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
		PrimitiveScalarFunctionReuseAccOne.evalNum = 0;
	}

	@Test
	public void testReusePrimitiveScalarFunctionWithOrExpr() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "11-", "-"),
			Row.of(2, "22--", "--"),
			Row.of(3, "34---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("PrimitiveScalarFunctionReuseAcc", PrimitiveScalarFunctionReuseAcc.class);
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, PrimitiveScalarFunctionReuseAcc(i, b, s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
	}

	@Test
	public void testReusePrimitiveScalarFunctionWithOrExprOld() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 4L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "11-", "-"),
			Row.of(2, "22--", "--"),
			Row.of(3, "34---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("PrimitiveScalarFunctionReuseAcc", new PrimitiveScalarFunctionReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, b, PrimitiveScalarFunctionReuseAcc(i, b, s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where i = b or m is not null";
		execInsertSqlAndWaitResult(sql);

		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
	}

	@Test
	public void testReusePrimitiveScalarFunctionOld() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "11-", "-"),
			Row.of(2, "22--", "--"),
			Row.of(3, "33---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("PrimitiveScalarFunctionReuseAcc", new PrimitiveScalarFunctionReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, PrimitiveScalarFunctionReuseAcc(i, b, s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where m is not null";
		execInsertSqlAndWaitResult(sql);

		// 数据源条数 和 执行ScalarFunction的次数一致
		assertThat(sourceData.size(), equalTo(PrimitiveScalarFunctionReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionReuseAcc.evalNum = 0;
	}

	@Test
	public void testNoReusePrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "11-", "-"),
			Row.of(2, "22--", "--"),
			Row.of(3, "33---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().createTemporarySystemFunction("PrimitiveScalarFunctionNoReuseAcc", PrimitiveScalarFunctionNoReuseAcc.class);
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, PrimitiveScalarFunctionNoReuseAcc(i, b, s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where m is not null";
		execInsertSqlAndWaitResult(sql);

		assertThat(sourceData.size() * 2, equalTo(PrimitiveScalarFunctionNoReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionNoReuseAcc.evalNum = 0;
	}

	@Test
	public void testNoReusePrimitiveScalarFunctionOld() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "11-", "-"),
			Row.of(2, "22--", "--"),
			Row.of(3, "33---", "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, b STRING NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("PrimitiveScalarFunctionNoReuseAcc", new PrimitiveScalarFunctionNoReuseAcc());
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, PrimitiveScalarFunctionNoReuseAcc(i, b, s) as m, s FROM TestTable");
		String sql = "INSERT INTO sink SELECT i, m, s FROM tmp_Table where m is not null";
		execInsertSqlAndWaitResult(sql);

		assertThat(sourceData.size() * 2, equalTo(PrimitiveScalarFunctionNoReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		PrimitiveScalarFunctionNoReuseAcc.evalNum = 0;
	}

	@Test
	public void testReuseArrayPrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, "k1:v1"),
			Row.of(2, "k2:v2"),
			Row.of(3, "k3:v3")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "k1", "v1"),
			Row.of(2, "k2", "v2"),
			Row.of(3, "k3", "v3")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b STRING NOT NULL) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, k STRING NOT NULL, v STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("ArrayScalarFunctionReuseAcc", new ArrayScalarFunctionAcc(true));
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, ArrayScalarFunctionReuseAcc(b) as items FROM TestTable");
		execInsertSqlAndWaitResult("INSERT INTO sink SELECT i, items[1] as k, items[2] as v FROM tmp_Table where items is not null");

		assertThat(sourceData.size(), equalTo(ArrayScalarFunctionAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		ArrayScalarFunctionAcc.evalNum = 0;
	}

	@Test
	public void testNoReuseArrayPrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, "k1:v1"),
			Row.of(2, "k2:v2"),
			Row.of(3, "k3:v3")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "k1", "v1"),
			Row.of(2, "k2", "v2"),
			Row.of(3, "k3", "v3")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b STRING NOT NULL) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, k STRING NOT NULL, v STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("ArrayScalarFunctionNoUseAcc", new ArrayScalarFunctionAcc(false));
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, ArrayScalarFunctionNoUseAcc(b) as items FROM TestTable");
		execInsertSqlAndWaitResult("INSERT INTO sink SELECT i, items[1] as k, items[2] as v FROM tmp_Table where items is not null");

		// 调用了三次 `items[1]`  `items[2]`  `items is not null`
		assertThat(sourceData.size() * 3, equalTo(ArrayScalarFunctionAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		ArrayScalarFunctionAcc.evalNum = 0;
	}

	@Test
	public void testReuseMapPrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, "c1:v1,c2:m1"),
			Row.of(2, "c1:v2,c2:m2"),
			Row.of(3, "c1:v3,c2:m3")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "v1", "m1"),
			Row.of(2, "v2", "m2"),
			Row.of(3, "v3", "m3")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b STRING NOT NULL) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, k STRING NOT NULL, v STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("MapScalarFunctionReuseAcc", new MapScalarFunctionNoReuseAcc(true));
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, MapScalarFunctionReuseAcc(b) as items FROM TestTable");
		execInsertSqlAndWaitResult("INSERT INTO sink SELECT i, items['c1'] as k, items['c2'] as v FROM tmp_Table where items is not null");

		assertThat(sourceData.size(), equalTo(MapScalarFunctionNoReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		MapScalarFunctionNoReuseAcc.evalNum = 0;
	}

	@Test
	public void testNoReuseMapPrimitiveScalarFunction() {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, "c1:v1,c2:m1"),
			Row.of(2, "c1:v2,c2:m2"),
			Row.of(3, "c1:v3,c2:m3")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, "v1", "m1"),
			Row.of(2, "v2", "m2"),
			Row.of(3, "v3", "m3")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b STRING NOT NULL) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE sink(i INT NOT NULL, k STRING NOT NULL, v STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().registerFunction("MapScalarFunctionReuseAcc", new MapScalarFunctionNoReuseAcc(false));
		tEnv().executeSql("CREATE view tmp_Table as SELECT i, MapScalarFunctionReuseAcc(b) as items FROM TestTable");
		execInsertSqlAndWaitResult("INSERT INTO sink SELECT i, items['c1'] as k, items['c2'] as v FROM tmp_Table where items is not null");

		// items['c1']
		// items['c2']
		// items is not null
		assertThat(sourceData.size() * 3, equalTo(MapScalarFunctionNoReuseAcc.evalNum));
		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
		MapScalarFunctionNoReuseAcc.evalNum = 0;
	}

	public static class PrimitiveScalarFunctionReuseAcc extends ScalarFunction {
		public static int evalNum = 0;

		public String eval(int i, long l, String s) {
			evalNum++;
			return String.valueOf(i) + l + s;
		}

		@Override
		public boolean isDeterministic() {
			return true;
		}
	}

	public static class PrimitiveScalarFunctionReuseAccOne extends ScalarFunction {
		public static int evalNum = 0;

		public long eval(int i, long l, String s) {
			evalNum++;
			return i + l + s.length();
		}

		@Override
		public boolean isDeterministic() {
			return true;
		}
	}

	public static class PrimitiveScalarFunctionNoReuseAcc extends ScalarFunction {
		public static int evalNum = 0;

		public String eval(int i, long l, String s) {
			evalNum++;
			return String.valueOf(i) + l + s;
		}

		@Override
		public boolean isDeterministic() {
			// 模拟random()，每次计算结果不一样，使得复用逻辑无法复用
			return false;
		}
	}

	public static class ArrayScalarFunctionAcc extends ScalarFunction {
		public static int evalNum = 0;
		private boolean isDeterministic;

		public ArrayScalarFunctionAcc(boolean isDeterministic) {
			this.isDeterministic = isDeterministic;
		}

		public String[] eval(String str) {
			evalNum++;
			return str.split(":");
		}

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return ObjectArrayTypeInfo.getInfoFor(Types.STRING);
		}

		@Override
		public boolean isDeterministic() {
			return isDeterministic;
		}
	}

	public static class MapScalarFunctionNoReuseAcc extends ScalarFunction {
		public static int evalNum = 0;
		private boolean isDeterministic;

		public MapScalarFunctionNoReuseAcc(boolean isDeterministic) {
			this.isDeterministic = isDeterministic;
		}

		public Map<String, String> eval(String str) {
			evalNum++;
			Map<String, String> map = new HashMap<>();
			for (String s : str.split(",")) {
				String[] split = s.split(":");
				map.put(split[0], split[1]);
			}
			return map;
		}

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return Types.MAP(Types.STRING, Types.STRING);
		}

		@Override
		public boolean isDeterministic() {
			return isDeterministic;
		}
	}

}
