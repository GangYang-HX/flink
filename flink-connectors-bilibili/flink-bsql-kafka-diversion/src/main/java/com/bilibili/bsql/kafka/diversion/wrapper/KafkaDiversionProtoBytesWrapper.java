package com.bilibili.bsql.kafka.diversion.wrapper;

import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.util.List;

public class KafkaDiversionProtoBytesWrapper extends KafkaDiversionWrapper{

	private final PbRowDataSerializationSchema serializationSchema;

	public KafkaDiversionProtoBytesWrapper(PbRowDataSerializationSchema serializationSchema, int keyIndex, DynamicTableSink.DataStructureConverter converter, int[] topicFieldIndices, Class<?> topicUdfClass, int[] brokerFieldIndices, Class<?> brokerUdfClass, int cacheTtl, boolean excludeField, int[] metaColumnIndices, List<String> metadataKeys) {
		super(serializationSchema, keyIndex, converter, topicFieldIndices, topicUdfClass, brokerFieldIndices, brokerUdfClass, cacheTtl, excludeField, metaColumnIndices, metadataKeys);
		this.serializationSchema = serializationSchema;
	}

	@Override
	public kafkaRecord generateRecord(RowData element) {
		Row row = (Row) converter.toExternal(element);
		byte[] keyBytes = this.serializeKey1(row);
		byte[] valueBytes = this.serializeValue(element);
		return new kafkaRecord(keyBytes, valueBytes);
	}

	@Override
	public byte[] serializeValue(RowData element) {
		return this.serializationSchema.serialize(element);
	}
}
