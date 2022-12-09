package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2021/11/30 2:22 下午
 */
public class DefaultRowConverter {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultRowConverter.class);
	private final RowType rowType;

	public DefaultRowConverter(RowType rowType){
		this.rowType = rowType;
	}

	public RowData convertDefaultRow() {
		GenericRowData row = new GenericRowData(rowType.getFieldCount());
		for (int i = 0; i < rowType.getFieldCount(); i++) {
			RowType.RowField field = rowType.getFields().get(i);
			try {
				row.setField(i, getDefaultValue(field.getType()));
			} catch (Exception e) {
				// ignore
				LOG.warn("set default row failed,cause by ", e);

			}
		}
		return row;
	}

	public static Object getDefaultValue(LogicalType type) {
		if (type instanceof IntType) {
			return new Integer(0);
		}
		if (type instanceof BigIntType) {
			return new Long(0);
		}
		if (type instanceof CharType || type instanceof VarCharType) {
			return StringData.fromString("");
		}
		if (type instanceof TimestampType) {
			// when play back from checkpoint, if a 'now' value appear, can push the window forward
			// which will cause many record dropped
			return TimestampData.fromEpochMillis(0);
		}
		if (type instanceof TinyIntType) {
			return new Byte((byte) 0);
		}
		if (type instanceof SmallIntType) {
			return new Short((short) 0);
		}
		if (type instanceof FloatType) {
			return new Float(0);
		}
		if (type instanceof DoubleType) {
			return new Double(0);
		}
		if (type instanceof VarBinaryType) {
			return new byte[0];
		}
		if (type instanceof ArrayType) {
			Object[] object = new Object[]{getDefaultValue(((ArrayType) type).getElementType())};
			return new GenericArrayData(object);
		}
		if (type instanceof RowType) {
			GenericRowData genericRowData = new GenericRowData(((RowType) type).getFieldCount());
			for (int i = 0; i < ((RowType) type).getFieldCount(); i++) {
				genericRowData.setField(i, getDefaultValue(((RowType) type).getFields().get(i).getType()));
			}
			return genericRowData;
		}
		if (type instanceof MapType) {
			Object key = getDefaultValue(((MapType) type).getKeyType());
			Object value = getDefaultValue(((MapType) type).getValueType());
			Map<Object, Object> map = new HashMap<>();
			map.put(key, value);
			return new GenericMapData(map);
		}

		throw new UnsupportedOperationException("default type :" + type.toString() + "not support yet.");
	}

}
