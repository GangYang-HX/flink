package com.bilibili.bsql.format.delimit;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;

/** CustomDelimitBytesSerialization. */
public class CustomDelimitBytesSerialization implements SerializationSchema<RowData> {

    private TypeInformation<RowData> typeInfo;

    protected DynamicTableSink.DataStructureConverter converter;

    private final RowType rowType;

    public CustomDelimitBytesSerialization(
            RowType rowType,
            DynamicTableSink.DataStructureConverter converter,
            TypeInformation<RowData> typeInfo) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;
        this.converter = converter;
        this.rowType = rowType;
    }

    @Override
    public byte[] serialize(RowData element) {
        List<String> fieldNames = rowType.getFieldNames();
        if (fieldNames.size() > 1) {
            throw new IllegalStateException("only support one field for binary(varbinary) now.");
        }
        Row row = (Row) converter.toExternal(element);
        if (row.getArity() != fieldNames.size()) {
            throw new IllegalStateException(
                    String.format(
                            "Number of elements in the row '%s' is different from number of field names: %d",
                            row, fieldNames.size()));
        }
        Object fieldContent = row.getField(0);
        return (byte[]) fieldContent;
    }
}
