package com.bilibili.bsql.kafka.sink;

import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.util.List;

/** kafka sink for proto class */
public class HashKeyProtoBytesSerializationSchemaWrapper extends HashKeySerializationSchemaWrapper {

    private final PbRowDataSerializationSchema serializationSchema;

    public HashKeyProtoBytesSerializationSchemaWrapper(
            PbRowDataSerializationSchema serializationSchema,
            int keyIndex,
            DynamicTableSink.DataStructureConverter converter,
            int[] metaColumnIndices,
            List<String> metadataKeys) {
        super(serializationSchema, keyIndex, converter, metaColumnIndices, metadataKeys);
        this.serializationSchema = serializationSchema;
    }

    @Override
    public byte[] serializeKey(RowData element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] serializeValue(RowData element) {
        return this.serializationSchema.serialize(element);
    }

    @Override
    public String getTargetTopic(RowData element) {
        return null;
    }

    @Override
    public kafkaRecord generateRecord(RowData element) {
        Row row = (Row) converter.toExternal(element);
        byte[] keyBytes = this.serializeKey1(row);
        byte[] valueBytes = this.serializeValue(element);
        return new kafkaRecord(keyBytes, valueBytes);
    }

    @Override
    protected byte[] serializeKey1(Row element) {
        return super.serializeKey1(element);
    }

    @Override
    protected byte[] serializeValue1(Row row) {
        throw new UnsupportedOperationException();
    }
}
