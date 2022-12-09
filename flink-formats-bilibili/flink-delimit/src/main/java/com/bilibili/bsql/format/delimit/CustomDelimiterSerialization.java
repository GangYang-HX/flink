
package com.bilibili.bsql.format.delimit;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import static com.bilibili.bsql.format.delimit.utils.DelimitStringUtils.unicodeStringDecode;

/** CustomDelimiterSerialization. */
public class CustomDelimiterSerialization implements SerializationSchema<RowData> {

    private static final long serialVersionUID = -2885556750743978939L;

    private TypeInformation<RowData> typeInfo;

    public final String delimiterKey;

    public CustomDelimiterSerialization(TypeInformation<RowData> typeInfo, String delimiterKey) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;
        this.delimiterKey = unicodeStringDecode(delimiterKey);
    }

    @Override
    public byte[] serialize(RowData element) {
        /** nothing done here */
        throw new UnsupportedOperationException();
    }
}
