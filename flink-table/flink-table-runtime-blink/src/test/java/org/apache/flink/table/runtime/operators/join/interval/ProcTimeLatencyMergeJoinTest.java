/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.interval;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.latency.ProcTimeGlobalJoin;
import org.apache.flink.table.runtime.operators.join.latency.config.GlobalJoinConfig;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 *
 * @author zhouxiaogang
 * @version $Id: ProcTimeLatencyMergeJoinTest.java, v 0.1 2020-12-01 11:50
zhouxiaogang Exp $$
 */
public class ProcTimeLatencyMergeJoinTest{
	RowDataTypeInfo rowType = new RowDataTypeInfo(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));

	private RowDataTypeInfo outputRowType = new RowDataTypeInfo(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
		new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
	RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(outputRowType.getFieldTypes());

	private String funcCode =
		"public class IntervalJoinFunction\n" +
			"    extends org.apache.flink.api.common.functions.RichFlatJoinFunction {\n" +
			"  final org.apache.flink.table.data.JoinedRowData joinedRow = new org.apache.flink.table.data.JoinedRowData();\n" +

			"  public IntervalJoinFunction(Object[] references) throws Exception {}\n" +

			"  @Override\n" +
			"  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {}\n" +

			"  @Override\n" +
			"  public void join(Object _in1, Object _in2, org.apache.flink.util.Collector c) throws Exception {\n" +
			"    org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) _in1;\n" +
			"    org.apache.flink.table.data.RowData in2 = (org.apache.flink.table.data.RowData) _in2;\n" +
			"    joinedRow.replace(in1,in2);\n" +
			"    c.collect(joinedRow);\n" +
			"  }\n" +

			"  @Override\n" +
			"  public void close() throws Exception {}\n" +
			"}\n";

	GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> generatedFunction = new GeneratedFunction(
		"IntervalJoinFunction", funcCode, new Object[0]);

	private int keyIdx = 0;
	private BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[] { keyIdx },
		rowType.getLogicalTypes());
	private TypeInformation<RowData> keyType = new RowDataTypeInfo();
	RowDataTypeInfo returnRowType = new RowDataTypeInfo(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH),
		new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));


	@Test
	public void testRightMergeJoin() throws Exception {
		GlobalJoinConfig config = new GlobalJoinConfig();
		//todo: set jobId/ joinNo/ merge
		config.setDelayMs(30);
		config.setWindowLengthMs(60);
		config.setBucketNum(400);
		config.setRedisType(1);
		config.setCompressType(3);
		config.setGlobalLatency(true);
		config.setRedisAddress("172.22.33.30:7899");
		config.setMergeSideFunc("merge");


		ProcTimeGlobalJoin joinProcessFunc = new ProcTimeGlobalJoin(
			FlinkJoinType.INNER, config, rowType, rowType, returnRowType, generatedFunction, keySelector, keySelector);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
			joinProcessFunc);

		testHarness.open();
		testHarness.processElement1(insertRecord("1L", "1a1"));
		testHarness.processElement2(insertRecord("1L", "1b1"));
		testHarness.processElement2(insertRecord("1L", "1b2"));

		testHarness.setProcessingTime(Clock.systemDefaultZone().millis() + 61000);
		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord("1L", "1a1", "1L,1L", "1b1,1b2"));

		System.err.println(output);

		assertor.assertOutputEquals("output wrong.", expectedOutput, output);
		testHarness.close();

	}

	private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> createTestHarness(
		ProcTimeGlobalJoin intervalLatencyJoinFunc)
		throws Exception {
		KeyedCoProcessOperator<RowData, RowData, RowData, RowData> operator = new KeyedCoProcessOperator<>(
			intervalLatencyJoinFunc);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
			new KeyedTwoInputStreamOperatorTestHarness<>(operator, keySelector, keySelector, keyType);
		return testHarness;
	}
}
