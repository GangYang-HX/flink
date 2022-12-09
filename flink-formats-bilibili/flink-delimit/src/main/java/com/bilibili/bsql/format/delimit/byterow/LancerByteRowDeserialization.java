package com.bilibili.bsql.format.delimit.byterow; // package bsql.delimit.format.byterow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.trace.LogTracer;

import java.io.IOException;

/** LancerByteRowDeserialization. */
public class LancerByteRowDeserialization extends ByteRowDeserialization implements LogTracer {

    public LancerByteRowDeserialization(
            TypeInformation<RowData> typeInfo, RowType rowType, String delimiterKey) {
        super(typeInfo, rowType, delimiterKey);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        return super.deserialize(message);
    }
}
