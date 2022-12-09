package com.bilibili.bsql.format.delimit;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.bilibili.bsql.format.delimit.utils.ObjectUtil;

import java.time.LocalDateTime;
import java.util.List;

import static com.bilibili.bsql.format.delimit.utils.DelimitStringUtils.unicodeStringDecode;

/** CustomDelimiterSerialization. */
public class CustomDelimiterSerialization implements SerializationSchema<RowData> {

    private static final long serialVersionUID = -2885556750743978939L;

    private TypeInformation<RowData> typeInfo;

    protected DynamicTableSink.DataStructureConverter converter;

    public final String delimiterKey;

    private final RowType rowType;

    public CustomDelimiterSerialization(
            RowType rowType,
            DynamicTableSink.DataStructureConverter converter,
            TypeInformation<RowData> typeInfo,
            String delimiterKey) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;
        this.delimiterKey = unicodeStringDecode(delimiterKey);
        this.converter = converter;
        this.rowType = rowType;
    }

    @Override
    public byte[] serialize(RowData element) {
        List<String> fieldNames = rowType.getFieldNames();
        Row row = (Row) converter.toExternal(element);
        if (element.getArity() != fieldNames.size()) {
            throw new IllegalStateException(
                    String.format(
                            "Number of elements in the row '%s' is different from number of field names: %d",
                            row, fieldNames.size()));
        }

        try {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < fieldNames.size(); i++) {
                Object fieldContent = row.getField(i);
                if (fieldContent instanceof LocalDateTime) {
                    fieldContent = ObjectUtil.getLocalDateTimestamp((LocalDateTime) fieldContent);
                }
                stringBuilder.append(fieldContent == null ? "" : fieldContent.toString());
                stringBuilder.append(i != (fieldNames.size() - 1) ? delimiterKey : "");
            }
            return stringBuilder.toString().getBytes();
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Could not serialize row '"
                            + row
                            + "'. "
                            + "Make sure that the schema matches the input.",
                    t);
        }
    }
}
