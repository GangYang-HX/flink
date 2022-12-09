package com.bilibili.bsql.hive.filetype.orc.vector;

import org.apache.flink.types.Row;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;

/**
 * Created by haozhugogo on 2020/5/6.
 */
public class RowVectorizer extends Vectorizer<Row> implements Serializable {

	private final Integer metaPos;
	public RowVectorizer(String schema) {
		super(schema);
		metaPos = -1;
	}

	public RowVectorizer(String schema , Integer metaPos) {
		super(schema);
		this.metaPos = metaPos;
	}

	@Override
	public void vectorize(Row row, VectorizedRowBatch batch) throws IOException {
		int dataSize = batch.size++;

		for (int i = 0; i < row.getArity(); i++) {
			if (metaPos == i) { // skip meta field
				continue;
			}
			ColumnVector cv = batch.cols[i];
			Object field = row.getField(i);
			setColumn(dataSize, cv, field);
		}

	}


	private void setColumn(int rowId, ColumnVector cv, Object field) throws IOException {
		if (field == null) {
			cv.noNulls = false;
			cv.isNull[rowId] = true;
			return;
		}
		String javaClassName = field.getClass().getSimpleName();

		if (cv instanceof LongColumnVector) {
			LongColumnVector longColumnVector = (LongColumnVector) cv;
			switch (javaClassName) {
				case "Boolean":
					longColumnVector.vector[rowId] = (Boolean) field ? 1L : 0L;
					break;
				case "Byte":
					longColumnVector.vector[rowId] = ((Byte) field).longValue();
					break;
				case "Date":
					longColumnVector.vector[rowId] = ((Date) field).getTime();
					break;
				case "Short":
					longColumnVector.vector[rowId] = ((Short) field).longValue();
					break;
				case "Integer":
					longColumnVector.vector[rowId] = ((Integer) field).longValue();
					break;
				case "Long":
					longColumnVector.vector[rowId] = (Long) field;
					break;
				default:
					throw new IOException("java class:" + javaClassName + ",LongColumnVector not match!");
			}
		} else if (cv instanceof DoubleColumnVector) {
			DoubleColumnVector doubleColumnVector = (DoubleColumnVector) cv;
			switch (javaClassName) {
				case "Float":
					doubleColumnVector.vector[rowId] = ((Float) field).doubleValue();
					break;
				case "Double":
					doubleColumnVector.vector[rowId] = (Double) field;
					break;
				default:
					throw new IOException("java class:" + javaClassName + ",DoubleColumnVector not match!");
			}
		} else if (cv instanceof BytesColumnVector) {
			BytesColumnVector bytesColumnVector = (BytesColumnVector) cv;
			switch (javaClassName) {
				case "String":
					bytesColumnVector.setVal(rowId, ((String) field).getBytes());
					break;
				case "byte[]":
					bytesColumnVector.setVal(rowId, (byte[]) field);
					break;
				default:
					throw new IOException("java class:" + javaClassName + ",BytesColumnVector not match!");
			}
		} else if (cv instanceof DecimalColumnVector) {
			DecimalColumnVector decimalColumnVector = (DecimalColumnVector) cv;
			decimalColumnVector.vector[rowId] = new HiveDecimalWritable(((BigDecimal) field).longValue());
		} else if (cv instanceof TimestampColumnVector) {
			TimestampColumnVector timestampColumnVector = (TimestampColumnVector) cv;
			Timestamp timestamp = field instanceof LocalDateTime ?
					Timestamp.valueOf((LocalDateTime) field) : (Timestamp) field;
			timestampColumnVector.set(rowId, timestamp);
		} else if (cv instanceof ListColumnVector) {
			ListColumnVector vector = (ListColumnVector) cv;
			int offset = vector.childCount;
			int length = ((Object[]) field).length;
			vector.offsets[rowId] = offset;
			vector.lengths[rowId] = length;
			vector.child.ensureSize(offset + length, true);
			vector.childCount += length;
			for (int c = 0; c < length; ++c) {
				setColumn(offset + c, vector.child, ((Object[]) field)[c]);
			}
		} else if(cv instanceof StructColumnVector){
			StructColumnVector vector = (StructColumnVector) cv;
			ColumnVector[] columnVectors = vector.fields;
			for (int c = 0; c < columnVectors.length; ++c) {
				ColumnVector columnVector = columnVectors[c];
				setColumn(rowId, columnVector,
					((Row)field).getField(c));
			}
		} else if(cv instanceof UnionColumnVector){
			throw new IOException("not support ColumnVector:" + cv.getClass().getName());
		} else if(cv instanceof MapColumnVector){
			MapColumnVector vector = (MapColumnVector) cv;

			int offset = vector.childCount;
			Set entrySet = ((Map) field).entrySet();
			int length = entrySet.size();
			vector.offsets[rowId] = offset;
			vector.lengths[rowId] = length;
			vector.keys.ensureSize(offset + length, true);
			vector.values.ensureSize(offset + length, true);
			vector.childCount += length;
			for (Object item: entrySet) {
				Map.Entry pair = (Map.Entry) item;
				setColumn(offset, vector.keys, pair.getKey());
				setColumn(offset, vector.values, pair.getValue());
				offset += 1;
			}
		} else {
			throw new IOException("not support ColumnVector:" + cv.getClass().getName());
		}
	}

}
