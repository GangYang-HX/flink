
package com.bilibili.bsql.format.delimit.byterow; // package bsql.delimit.format.byterow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.trace.LancerTrace;
import com.bilibili.bsql.trace.LogTracer;
import com.bilibili.bsql.trace.Trace;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** LancerByteRowDeserialization. */
public class LancerByteRowDeserialization extends ByteRowDeserialization implements LogTracer {

    private Trace trace;
    private String sinkDest = "";
    private String traceId = "";
    private Map<String, String> headers = new HashMap<>();

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public LancerByteRowDeserialization(
            TypeInformation<RowData> typeInfo,
            RowType rowType,
            String delimiterKey,
            String sinkDest,
            String traceId) {
        super(typeInfo, rowType, delimiterKey);
        this.sinkDest = sinkDest;
        this.traceId = traceId;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        this.trace = new LancerTrace();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        //		logTraceEvent(trace, headers, sinkDest);
        return super.deserialize(message);
    }
}
