/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package com.bilibili.bsql.common;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import java.sql.Timestamp;

import org.junit.Assert;
import org.junit.Test;

import com.bilibili.bsql.common.format.CustomDelimiterDeserialization;
import com.bilibili.bsql.common.format.raw.CustomDelimiterRawDeserialization;

/**
 *
 * @author zhouxiaogang
 * @version $Id: DelimiterTest.java, v 0.1 2021-03-18 18:43
zhouxiaogang Exp $$
 */
public class DelimiterTest {
	@Test
	public void normalDelimitTest() throws Exception {

		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(new VarCharType(), new IntType(), new TimestampType(6));
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new TimestampType(6)));


		CustomDelimiterDeserialization testDeser = new CustomDelimiterDeserialization(
			typeInfo,
			new RowType(rowFields),
			"|"
		);

		String demo = "laoluan|2|";
		demo += new Timestamp(5);


		RowData data = testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);
	}

	@Test
	public void normalRawDelimitMultiRecordTest() throws Exception {

		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(new VarCharType(), new IntType(), new TimestampType(6));
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new TimestampType(6)));


		CustomDelimiterRawDeserialization testDeser = new CustomDelimiterRawDeserialization(
			typeInfo,
			new RowType(rowFields),
			"|"
		);

		String demo = "llqd|-2|";
		demo += new Timestamp(5);

		testDeser.open(null);
		RowData data = testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);

		String demo1 = "llqd||";
		demo1 += new Timestamp(5);

		RowData data1 = testDeser.deserialize(demo1.getBytes(StandardCharsets.UTF_8));
		System.err.println(data1);
	}

	@Test
	public void bytesDelimitTest() throws Exception {
		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(new VarCharType(), new IntType(), new VarBinaryType(6));
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new VarBinaryType()));


		CustomDelimiterRawDeserialization testDeser = new CustomDelimiterRawDeserialization(
			typeInfo,
			new RowType(rowFields),
			"|"
		);
		String demo = "llqd|-2|";
		demo += "abc";

		testDeser.open(null);
		RowData data = testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);

		Assert.assertArrayEquals(new byte[]{97,98,99}, data.getBinary(2));
	}

	@Test
	public void recordShorterThanSchemaTest() throws Exception {
		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(new VarCharType(), new IntType(), new VarBinaryType(6), new IntType());
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new VarBinaryType()));
		rowFields.add(new RowType.RowField("d", new IntType()));


		CustomDelimiterRawDeserialization testDeser = new CustomDelimiterRawDeserialization(
			typeInfo,
			new RowType(rowFields),
			"|"
		);
		String demo = "llqd|-2|";
		demo += "abc";

		testDeser.open(null);
		GenericRowData data = (GenericRowData)testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);

		Assert.assertArrayEquals(new byte[]{97,98,99}, data.getBinary(2));
		Assert.assertSame(0, data.getInt(3));
	}

	@Test
	public void recordLongerThanSchemaTest() throws Exception {
		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(new VarCharType(), new IntType(), new VarBinaryType(6));
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new VarBinaryType()));


		CustomDelimiterRawDeserialization testDeser = new CustomDelimiterRawDeserialization(
			typeInfo,
			new RowType(rowFields),
			"|"
		);
		String demo = "llqd|-2|";
		demo += "abc|";
		demo += "555";

		testDeser.open(null);
		RowData data = testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);

		Assert.assertTrue(testDeser.hasSomeUnexpected);
		Assert.assertArrayEquals(new byte[]{97,98,99}, data.getBinary(2));
	}

	@Test
	public void recordIncompatibleSchemaTest() throws Exception {
		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(new VarCharType(), new IntType(), new TimestampType(6), new IntType());
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new VarBinaryType()));
		rowFields.add(new RowType.RowField("d", new IntType()));


		CustomDelimiterRawDeserialization testDeser = new CustomDelimiterRawDeserialization(
			typeInfo,
			new RowType(rowFields),
			"|"
		);
		String demo = "llqd||";
		demo += "abc|";
		demo += "llgl";

		testDeser.open(null);
		RowData data = testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);

		Assert.assertTrue(testDeser.hasSomeUnexpected);
		Assert.assertArrayEquals(new byte[]{97,98,99}, data.getBinary(2));
	}

	@Test
	public void nullValueNormalTest() throws Exception {
		RowDataTypeInfo typeInfo =
			new RowDataTypeInfo(
				new VarCharType(),
				new IntType(),
				new TimestampType(6),
				new BigIntType(),
				new BooleanType(),
				new TinyIntType(),
				new SmallIntType(),
				new FloatType(),
				new DoubleType()
			);
		ArrayList<RowType.RowField> rowFields = new ArrayList<RowType.RowField>();
		rowFields.add(new RowType.RowField("a", new VarCharType()));
		rowFields.add(new RowType.RowField("b", new IntType()));
		rowFields.add(new RowType.RowField("c", new VarBinaryType()));
		rowFields.add(new RowType.RowField("d", new BigIntType()));
		rowFields.add(new RowType.RowField("e", new BooleanType()));
		rowFields.add(new RowType.RowField("f", new TinyIntType()));
		rowFields.add(new RowType.RowField("g", new SmallIntType()));
		rowFields.add(new RowType.RowField("h", new FloatType()));
		rowFields.add(new RowType.RowField("i", new DoubleType()));

		CustomDelimiterRawDeserialization testDeser = new CustomDelimiterRawDeserialization(
			typeInfo,
			new RowType(rowFields),
			"\u0001"
		);
		String demo = "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001";


		testDeser.open(null);
		RowData data = testDeser.deserialize(demo.getBytes(StandardCharsets.UTF_8));
		System.err.println(data);

		Assert.assertEquals("", data.getString(0).toString());
		Assert.assertEquals(0, data.getInt(1));
		Assert.assertArrayEquals(new byte[]{}, data.getBinary(2));
		Assert.assertEquals(0, data.getLong(3));
		Assert.assertEquals(false, data.getBoolean(4));
		Assert.assertEquals(0, data.getByte(5));
		Assert.assertEquals(0, data.getShort(6));
		Assert.assertEquals(0.0, data.getFloat(7),0);
		Assert.assertEquals(0.0, data.getDouble(8), 0);

		Assert.assertFalse(testDeser.hasSomeUnexpected);
	}
}
