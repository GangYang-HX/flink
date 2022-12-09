/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.sink;

import com.bilibili.bsql.common.format.CustomDelimiterSerialization;
import com.bilibili.bsql.kafka.ability.metadata.write.WritableMetadata;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.bilibili.bsql.common.utils.BinaryTransform.encodeBigEndian;
import static com.bilibili.bsql.common.utils.ObjectUtil.getLocalDateTimestamp;
import static com.bilibili.bsql.kafka.tableinfo.KafkaSinkTableInfo.UNDEFINED_IDX;


/**
 *
 * @author zhouxiaogang
 * @version $Id: CustomSerializationSchemaWrapper.java, v 0.1 2020-03-05 16:33
zhouxiaogang Exp $$
 */
public class HashKeySerializationSchemaWrapper extends KeyedSerializationSchemaWrapper<RowData> {
	protected int keyIndex;
	protected DynamicTableSink.DataStructureConverter converter;
	protected TypeInformation<RowData> typeInfo;
	protected final String delimiterKey;
	protected final int[] metaColumnIndices;
	protected final List<String> metadataKeys;
	protected final int[] metadataSortedPositions;

	public HashKeySerializationSchemaWrapper(CustomDelimiterSerialization serializationSchema,
											 int keyIndex,
											 DynamicTableSink.DataStructureConverter converter,
											 int[] metaColumnIndices,
											 List<String> metadataKeys) {
		super(serializationSchema);
		this.keyIndex = keyIndex;
		this.converter = converter;
		this.typeInfo = serializationSchema.typeInfo;
		this.delimiterKey = serializationSchema.delimiterKey;
		this.metaColumnIndices = metaColumnIndices;
		this.metadataKeys = metadataKeys;
		this.metadataSortedPositions = initMetadataSortedPositions();
	}

	public HashKeySerializationSchemaWrapper(SerializationSchema<RowData> serializationSchema,
											 int keyIndex,
											 DynamicTableSink.DataStructureConverter converter,
											 int[] metaColumnIndices,
											 List<String> metadataKeys) {
		super(serializationSchema);
		this.keyIndex = keyIndex;
		this.converter = converter;
        this.typeInfo = null;
		this.delimiterKey = null;
		this.metaColumnIndices = metaColumnIndices;
		this.metadataKeys = metadataKeys;
		this.metadataSortedPositions = initMetadataSortedPositions();
	}

    private int[] initMetadataSortedPositions() {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        m -> {
                            final int pos = metadataKeys.indexOf(m.key);
                            if (pos < 0) {
                                return -1;
                            } else {
                                return metaColumnIndices[pos];
                            }
                        })
                .toArray();
	}

	public byte[] serializeKey(RowData element) {
		/**
		 * no thing done here
		 *
		 * */
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

	public kafkaRecord generateRecord(RowData element) {
		Row row = (Row)converter.toExternal(element);
		if (metaColumnIndices.length != 0) {
			Row physicalRow = new Row(row.getKind(), row.getArity() - metaColumnIndices.length);
			int physicalIndex = 0;
			for (int i = 0; i< row.getArity(); i++) {
				int loopIndex = i;
				if (Arrays.stream(metaColumnIndices).noneMatch(index -> index == loopIndex)) {
					physicalRow.setField(physicalIndex++ , row.getField(loopIndex));
				}
			}
			return new kafkaRecord(
				serializeKey1(physicalRow),
				serializeValue1(physicalRow)
			);
		}
		return new kafkaRecord(
			serializeKey1(row),
			serializeValue1(row)
		);

	}

	public <T> T readMetaData(RowData producedRowData, WritableMetadata metadata) {
		final int pos = metadataSortedPositions[metadata.ordinal()];
		if (pos < 0) {
			return null;
		}
		return (T) metadata.converter.read(producedRowData, pos);
	}

	protected byte[] serializeKey1(Row element) {
		if (keyIndex == UNDEFINED_IDX) {
			return new byte[4];
		}
		Object fieldValue = element.getField(keyIndex);
		return encodeBigEndian(hash(fieldValue));
	}

	protected byte[] serializeValue1(Row row) {
		final String[] fieldNames = ((RowDataTypeInfo) typeInfo).getFieldNames();
		if (row.getArity() != fieldNames.length) {
			throw new IllegalStateException(
				String.format("Number of elements in the row '%s' is different from number of field names: %d",
					row, fieldNames.length));
		}

		try {
			StringBuilder stringBuilder = new StringBuilder();

			for (int i = 0; i < fieldNames.length; i++) {
				Object fieldContent = row.getField(i);
				/**
				 * saber use the timestamp to sink the time, if it is the localdatetime type
				 * we need to convert it to the timestamp type
				 * */
				if (fieldContent instanceof LocalDateTime) {
					fieldContent = getLocalDateTimestamp((LocalDateTime)fieldContent);
				}
				stringBuilder.append(fieldContent == null ? "" : fieldContent.toString());
				stringBuilder.append(i != (fieldNames.length - 1) ? delimiterKey : "");
			}

			return stringBuilder.toString().getBytes();

		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'. "
				+ "Make sure that the schema matches the input.", t);
		}
	}


	public static class kafkaRecord {
		public byte[] key;
		public byte[] value;

		public kafkaRecord(byte[] key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}

	static final int hash(Object key) {
		int h;
		return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
	}

	public DynamicTableSink.DataStructureConverter getConverter() {
		return converter;
	}
}
