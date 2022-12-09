/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.interval;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.latency.ProcTimeGlobalJoinWithTTL;
import org.apache.flink.table.runtime.operators.join.latency.cache.JoinCacheEnums;
import org.apache.flink.table.runtime.operators.join.latency.config.GlobalJoinConfig;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 *
 * @author zhouxiaogang
 * @version $Id: ProcTimeLatencyJoinTest.java, v 0.1 2020-11-23 12:02
zhouxiaogang Exp $$
 */
public class ProcTimeGlobalJoinTest extends TimeIntervalStreamJoinTestBase {
	private int keyIdx = 0;
	private BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[] { keyIdx },
		rowType.getLogicalTypes());
	private TypeInformation<RowData> keyType = new RowDataTypeInfo();
	RowDataTypeInfo returnRowType = new RowDataTypeInfo(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH),
		new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));


	@Test
	public void testImmediateCacheHitAndOutput() throws Exception {
		GlobalJoinConfig config = new GlobalJoinConfig();
		//todo: set jobId/ joinNo/ merge
		config.setDelayMs(30);
		config.setWindowLengthMs(60);
		config.setBucketNum(400);
		config.setRedisType(1);
		config.setCompressType(3);
		config.setGlobalLatency(false);
		config.setRedisAddress("172.22.33.30:7899");


		ProcTimeGlobalJoinWithTTL joinProcessFunc = new ProcTimeGlobalJoinWithTTL(
			FlinkJoinType.INNER, config, rowType, rowType, returnRowType, generatedFunction, keySelector, keySelector);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
			joinProcessFunc);

		testHarness.open();
		testHarness.processElement2(insertRecord(1L, "1b1"));
		testHarness.processElement1(insertRecord(1L, "1a1"));
		testHarness.setProcessingTime(1);
		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1L, "1a1", 1L, "1b1"));

		System.err.println(output);

		assertor.assertOutputEquals("output wrong.", expectedOutput, output);
		testHarness.close();

	}

	@Test
	public void testHoldAndHit() throws Exception {
		GlobalJoinConfig config = new GlobalJoinConfig();
		//todo: set jobId/ joinNo/ merge
		config.setDelayMs(60);
		config.setWindowLengthMs(30);
		config.setBucketNum(400);
		config.setRedisType(JoinCacheEnums.BUCKET.getCode());
		config.setCompressType(3);
		config.setGlobalLatency(true);
		config.setRedisAddress("172.22.33.30:7899");


		ProcTimeGlobalJoinWithTTL joinProcessFunc = new ProcTimeGlobalJoinWithTTL(
			FlinkJoinType.INNER, config,rowType, rowType, returnRowType, generatedFunction, keySelector, keySelector);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
			joinProcessFunc);

		testHarness.open();
		testHarness.processElement2(insertRecord(2L, "1b1"));
		testHarness.processElement1(insertRecord(2L, "1a1"));
		testHarness.setProcessingTime(1);
		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		Assert.assertTrue(output.size() == 0);

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(2L, "1a1", 2L, "1b1"));
		testHarness.setProcessingTime(Clock.systemDefaultZone().millis() + 50);
		output = testHarness.getOutput();
		System.err.println(output);

		assertor.assertOutputEquals("output wrong.", expectedOutput, output);
		testHarness.close();

	}

	@Test
	public void testMultiHoldAndHit() throws Exception {
		GlobalJoinConfig config = new GlobalJoinConfig();
		//todo: set jobId/ joinNo/ merge
		config.setDelayMs(60);
		config.setWindowLengthMs(30);
		config.setBucketNum(400);
		config.setRedisType(JoinCacheEnums.MULTI_BUCKET.getCode());
		config.setCompressType(3);
		config.setGlobalLatency(true);
		config.setRedisAddress("172.22.33.30:7899;172.23.47.13:7115");


		ProcTimeGlobalJoinWithTTL joinProcessFunc = new ProcTimeGlobalJoinWithTTL(
			FlinkJoinType.INNER, config,rowType, rowType, returnRowType, generatedFunction, keySelector, keySelector);



		for (int i = 0; i < 100; i++) {
			KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);
			testHarness.open();
			testHarness.processElement2(insertRecord((long)i, i + "b" + i));
			testHarness.processElement1(insertRecord((long)i, i + "a" + i));
			testHarness.setProcessingTime(1);
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			Assert.assertTrue(output.size() == 0);

			List<Object> expectedOutput = new ArrayList<>();
			expectedOutput.add(insertRecord((long)i, i + "a" + i, (long)i, i + "b" + i));
			testHarness.setProcessingTime(Clock.systemDefaultZone().millis() + 50);
			output = testHarness.getOutput();
			System.err.println(expectedOutput + ":" + output);

			assertor.assertOutputEquals("output wrong.", expectedOutput, output);
			testHarness.close();
		}


	}

	@Test
	public void testHoldAndHit2() throws Exception {
		GlobalJoinConfig config = new GlobalJoinConfig();
		//todo: set jobId/ joinNo/ merge
		config.setDelayMs(60);
		config.setWindowLengthMs(30);
		config.setBucketNum(400);
		config.setRedisType(JoinCacheEnums.BUCKET.getCode());
		config.setCompressType(3);
		config.setGlobalLatency(true);
		config.setMergeSideFunc("merge");
		config.setRedisAddress("172.22.33.30:7899");


		ProcTimeGlobalJoinWithTTL joinProcessFunc = new ProcTimeGlobalJoinWithTTL(
			FlinkJoinType.INNER, config,rowType, rowType, returnRowType, generatedFunction, keySelector, keySelector);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
			joinProcessFunc);

		testHarness.open();
//		testHarness.processElement1(insertRecord(2L, "1a1"));
		testHarness.processElement2(insertRecord(2L, "1b1"));
		testHarness.processElement2(insertRecord(2L, "1b1"));
		testHarness.setProcessingTime(1);
		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		Assert.assertTrue(output.size() == 0);

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(2L, "1a1", 2L, "1b1"));
		testHarness.setProcessingTime(Clock.systemDefaultZone().millis() + 50);
		output = testHarness.getOutput();
		System.err.println(output);

		assertor.assertOutputEquals("output wrong.", expectedOutput, output);
		testHarness.close();

	}

	private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> createTestHarness(
		ProcTimeGlobalJoinWithTTL intervalLatencyJoinFunc)
		throws Exception {
		KeyedCoProcessOperator<RowData, RowData, RowData, RowData> operator = new KeyedCoProcessOperator<>(
			intervalLatencyJoinFunc);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
			new KeyedTwoInputStreamOperatorTestHarness<>(operator, keySelector, keySelector, keyType);
		return testHarness;
	}

}
