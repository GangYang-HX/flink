package com.bilibili.bsql.kafka.sink;

import com.bilibili.bsql.common.format.CustomDelimiterSerialization;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author: zhuzhengjun
 * @date: 2021/7/7 3:15 下午
 */
public class HashKeyBytesSerializationSchemaWrapper extends HashKeySerializationSchemaWrapper{

	public HashKeyBytesSerializationSchemaWrapper(CustomDelimiterSerialization serializationSchema,
												  int keyIndex,
												  DynamicTableSink.DataStructureConverter converter,
												  int[] metaColumnIndices, List<String> metadataKeys) {
		super(serializationSchema, keyIndex, converter, metaColumnIndices, metadataKeys);
	}

	public byte[] serializeKey(RowData element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] serializeValue(RowData element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getTargetTopic(RowData element) {
		return null;
	}

	@Override
	protected byte[] serializeKey1(Row element) {
		return super.serializeKey1(element);
	}

	@Override
	protected byte[] serializeValue1(Row row) {
		final String[] fieldNames = ((RowDataTypeInfo) typeInfo).getFieldNames();
		if (fieldNames.length > 1) {
			throw new IllegalStateException("only support one field for binary(varbinary) now.");
		}
		if (row.getArity() != fieldNames.length) {
			throw new IllegalStateException(
				String.format("Number of elements in the row '%s' is different from number of field names: %d",
					row, fieldNames.length));
		}
		Object fieldContent = row.getField(0);
		return (byte[]) fieldContent;
	}

}
