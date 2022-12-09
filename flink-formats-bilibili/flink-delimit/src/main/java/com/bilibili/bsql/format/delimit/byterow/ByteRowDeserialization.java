
package com.bilibili.bsql.format.delimit.byterow;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.format.delimit.converter.CustomRowConverter;

import java.io.IOException;
import java.util.TimeZone;

import static com.bilibili.bsql.format.delimit.utils.DelimitStringUtils.unicodeStringDecode;

/** ByteRowDeserialization. */
public class ByteRowDeserialization extends AbstractDeserializationSchema<RowData> {
    private static final long serialVersionUID = 2385115520960444192L;

    private static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();
    private final String delimiterKey;
    private final RowType rowType;
    private final CustomRowConverter converter;
    protected int totalFieldsLength;

    public ByteRowDeserialization(
            TypeInformation<RowData> typeInfo, RowType rowType, String delimiterKey) {
        this.totalFieldsLength = typeInfo.getTotalFields();
        this.rowType = rowType;
        this.delimiterKey = unicodeStringDecode(delimiterKey);
        this.converter = new CustomRowConverter(rowType);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.converter.setRowConverterMetricGroup(context.getMetricGroup());
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, message);
        return row;
    }
}
