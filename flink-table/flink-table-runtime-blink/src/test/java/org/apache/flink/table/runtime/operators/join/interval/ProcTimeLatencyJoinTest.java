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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.latency.v2.ProcTimeLatencyJoin;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link ProcTimeIntervalJoin}.
 */
public class ProcTimeLatencyJoinTest extends TimeIntervalStreamJoinTestBase {

	private int keyIdx = 0;
	private BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[] { keyIdx },
			rowType.getLogicalTypes());
	private TypeInformation<RowData> keyType = new RowDataTypeInfo();


	/** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime + 20. **/
	@Test
	public void testProcTimeLatencyJoinWithCommonBounds() throws Exception {
		ProcTimeLatencyJoin joinProcessFunc = new ProcTimeLatencyJoin(
				FlinkJoinType.LEFT, -10, 20, rowType, rowType, generatedFunction, null);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);
		testHarness.open();
		testHarness.setProcessingTime(1);
		testHarness.processElement1(insertRecord(1L, "1a1"));
		assertEquals(1, testHarness.numProcessingTimeTimers());

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(1)***************************\n");

		testHarness.setProcessingTime(2);
		testHarness.processElement1(insertRecord(2L, "2a2"));
		assertEquals(2, testHarness.numProcessingTimeTimers());

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(2)***************************\n");


		testHarness.setProcessingTime(3);
		testHarness.processElement1(insertRecord(1L, "1a3"));
		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(3, testHarness.numProcessingTimeTimers());

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(3)***************************\n");

		testHarness.processElement2(insertRecord(1L, "1b3"));

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(3)***************************\n");

		testHarness.setProcessingTime(4);
		testHarness.processElement2(insertRecord(2L, "2b4"));
		assertEquals(6, testHarness.numKeyedStateEntries());
		assertEquals(3, testHarness.numProcessingTimeTimers());

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(4)***************************\n");

		testHarness.setProcessingTime(13);
		testHarness.processElement2(insertRecord(1L, "1b13"));

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(13)***************************\n");

		testHarness.setProcessingTime(33);

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(33)***************************\n");

		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());

		testHarness.processElement1(insertRecord(1L, "1a33"));
		testHarness.processElement1(insertRecord(2L, "2a33"));
		testHarness.processElement2(insertRecord(2L, "2b33"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1L, "1a1", 1L, "1b3"));
		expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b3"));
		expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b13"));
//		expectedOutput.add(insertRecord(2L, "2a2", 2L, "2b4"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	/** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5. **/
	@Test
	public void testProcTimeInnerJoinWithNegativeBounds() throws Exception {
		ProcTimeLatencyJoin joinProcessFunc = new ProcTimeLatencyJoin(
				FlinkJoinType.LEFT, -10, -5, rowType, rowType, generatedFunction, null);

		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness = createTestHarness(
				joinProcessFunc);
		testHarness.open();

		testHarness.setProcessingTime(1);
		testHarness.processElement1(insertRecord(1L, "1a1"));

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(1)***************************\n");

		testHarness.setProcessingTime(2);
		testHarness.processElement1(insertRecord(2L, "2a2"));

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(2)***************************\n");

		testHarness.setProcessingTime(3);
		testHarness.processElement1(insertRecord(1L, "1a3"));

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(3)***************************\n");

		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(3, testHarness.numProcessingTimeTimers());

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(3)***************************\n");

		testHarness.processElement2(insertRecord(1L, "1b3"));
		assertEquals(5, testHarness.numKeyedStateEntries());
		assertEquals(3, testHarness.numProcessingTimeTimers());

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(3)***************************\n");

		testHarness.setProcessingTime(7);

		System.out.println(testHarness.numProcessingTimeTimers());
		System.out.println(testHarness.numEventTimeTimers());
		System.out.println(testHarness.numKeyedStateEntries());
		System.out.println("setProcessingTime(7)***************************\n");

		testHarness.processElement2(insertRecord(2L, "2b7"));
		assertEquals(6, testHarness.numKeyedStateEntries());
		assertEquals(3, testHarness.numProcessingTimeTimers());

		testHarness.setProcessingTime(12);
		testHarness.processElement2(insertRecord(1L, "1b12"));

		testHarness.setProcessingTime(13);
		assertEquals(3, testHarness.numKeyedStateEntries());
		assertEquals(1, testHarness.numProcessingTimeTimers());

		testHarness.setProcessingTime(14);
		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());

		testHarness.setProcessingTime(16);
		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(2L, "2a2", 2L, "2b7"));
		expectedOutput.add(insertRecord(1L, "1a1", null, null));
		expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b12"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> createTestHarness(
			ProcTimeLatencyJoin latencyJoinFunc)
			throws Exception {
		KeyedCoProcessOperator<RowData, RowData, RowData, RowData> operator = new KeyedCoProcessOperator<>(
				latencyJoinFunc);
		KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(operator, keySelector, keySelector, keyType);
		return testHarness;
	}
}
