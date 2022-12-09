package com.bilibili.bsql.format.delimit;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/** CustomOtherDelimiterDeserialization. */
public class CustomOtherDelimiterDeserialization extends CustomDelimiterDeserialization {
    protected Boolean noDefaultValue;

    public CustomOtherDelimiterDeserialization(
            TypeInformation<RowData> typeInfo,
            RowType rowType,
            String delimiterKey,
            Boolean noDefaultValue) {
        super(typeInfo, rowType, delimiterKey);
        this.noDefaultValue = noDefaultValue;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        String inputString = new String(message);
        String[] inputSplit = StringUtils.splitPreserveAllTokens(inputString, delimiterKey);

        if (inputSplit == null) {
            throw new IOException("kafka deserialize failure");
        }
        if (this.noDefaultValue) {
            for (int i = 0; i < inputSplit.length; i++) {
                inputSplit[i] = StringUtils.trimToNull(inputSplit[i]);
            }
        }

        return converter.deserializeString(inputSplit);
    }
}
