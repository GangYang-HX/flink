package com.bilibili.bsql.common.format.raw;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.trace.LogTrace;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.bili.external.trace.LancerTrace;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.trace.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author weizefeng
 * @Date 2022/4/1 19:48
 **/
public class LancerCustomDelimiterRawDeserialization extends CustomDelimiterRawDeserialization implements LogTrace {
    private static final Logger LOG = LoggerFactory.getLogger(LancerCustomDelimiterRawDeserialization.class);
    private Trace trace;
    private String sinkDest;
    private String traceId;
    private Boolean useLancerDebug;
    private Map<String, String> headers = new HashMap<>();

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public LancerCustomDelimiterRawDeserialization(
        TypeInformation<RowData> typeInfo,
        RowType rowType,
        String delimiterKey,
        String sinkDest,
        String traceId,
        Boolean useLancerDebug
    ) {
        super(typeInfo, rowType, delimiterKey);
        this.sinkDest = sinkDest;
        this.traceId = traceId;
        this.useLancerDebug = useLancerDebug;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        this.trace = new LancerTrace(this.traceId);
        // datacenter ask field's default value = null
        Arrays.fill(toInternalDefault, null);
    }

    public RowData deserialize(byte[] message) throws IOException {

        hasSomeUnexpected = false;
        currentFieldStartPosition = 0;
        currentFieldIndexInRowData = 0;
        StringBuilder sb = new StringBuilder();

        GenericRowData parsedRowData = new GenericRowData(fieldTypes.length);

        MemorySegment[] memSegment = new MemorySegment[] {MemorySegmentFactory.wrap(message)};

        for (int position = 0; position < message.length; position++) {

            if (message[position] == 10){
                hasSomeUnexpected = true;
                logTraceError(trace, headers, TraceErrorCode.CONTAINS_NEWLINE_CHARACTER.errorCode, sinkDest);
            }

            if (currentFieldIndexInRowData >= rowType.getFieldCount()) {
                /*
                 * if the record has something not in the schema, just abrupt the deser process
                 * */
                hasSomeUnexpected = true;
                logTraceError(trace, headers, TraceErrorCode.TOO_MANY_FIELDS.errorCode, sinkDest);
                break;
            }

            byte current = message[position];
            if (current == delimiterKey) {
                Object currentFieldObject;
                try {
                    currentFieldObject = toInternalConverters[currentFieldIndexInRowData]
                        .deserialize(memSegment, currentFieldStartPosition, position - currentFieldStartPosition);
                } catch (Exception parseExp) {
                    hasSomeUnexpected = true;
                    currentFieldObject = toInternalDefault[currentFieldIndexInRowData];
                    logTraceError(trace, headers, TraceErrorCode.PARSE_ERROR.errorCode, sinkDest);
                    if (useLancerDebug) {
                        sb.append("error fieldName: ").append(rowType.getFieldNames().get(currentFieldIndexInRowData))
                          .append(" fieldType: ").append(rowType.getTypeAt(currentFieldIndexInRowData).getTypeRoot().name())
                          .append(" content: ").append(new String(Arrays.copyOfRange(message, currentFieldStartPosition, position)))
                          .append(";\n");
                    }
                }

                parsedRowData.setField(currentFieldIndexInRowData++, currentFieldObject);
                currentFieldStartPosition = position + 1;
                continue;
            }

            if (position == message.length - 1) {
                Object currentFieldObject;
                try {
                    currentFieldObject = toInternalConverters[currentFieldIndexInRowData]
                        .deserialize(memSegment, currentFieldStartPosition, position + 1 - currentFieldStartPosition);
                } catch (Exception parseExp) {
                    hasSomeUnexpected = true;
                    currentFieldObject = toInternalDefault[currentFieldIndexInRowData];
                    logTraceError(trace, headers, TraceErrorCode.PARSE_ERROR.errorCode, sinkDest);
                    if (useLancerDebug) {
                        sb.append("error fieldName: ").append(rowType.getFieldNames().get(currentFieldIndexInRowData))
                                .append(" fieldType: ").append(rowType.getTypeAt(currentFieldIndexInRowData).getTypeRoot().name())
                                .append(" content: ").append(new String(Arrays.copyOfRange(message, currentFieldStartPosition, position+1)))
                                .append(";\n");
                    }
                }

                parsedRowData.setField(currentFieldIndexInRowData++, currentFieldObject);
                break;
            }
        }


        /*
         * fill in the remaining field if the data is less than expected
         * if field count match, two value should match
         * */
        if (currentFieldIndexInRowData  < rowType.getFieldCount()) {
            hasSomeUnexpected = true;
            for (int i = currentFieldIndexInRowData; i < rowType.getFieldCount(); i++) {
                if (useLancerDebug) {
                    sb.append("miss fieldName: ").append(rowType.getFieldNames().get(i))
                            .append(" fieldType: ").append(rowType.getTypeAt(i).getTypeRoot().name())
                            .append(";\n");
                }
                logTraceError(trace, headers, TraceErrorCode.MISS_FIELD.errorCode, sinkDest);
                parsedRowData.setField(i, toInternalDefault[i]);
            }

            /*
             * if end with a delimit key, the last value is just empty, should not count as failure
             * */
            if (message.length == 0 || message[message.length - 1] != delimiterKey) {
                hasSomeUnexpected = true;
                logTraceError(trace, headers, TraceErrorCode.END_WITHOUT_DELIMITER_KEY.errorCode, sinkDest);
            }
        }

        if (hasSomeUnexpected && abnormalInputCounter != null) {
            abnormalInputCounter.inc();
        }

        if (useLancerDebug && hasSomeUnexpected) {
            List<String> fieldTypes = rowType.getFields().stream().map(o -> o.getType().getTypeRoot().name()).collect(Collectors.toList());
            LOG.info("RowData deserialize message:{}, fieldNames: {}, fieldTypes: {};\n{}", new String(message),
                    Arrays.toString(rowType.getFieldNames().toArray()),
                    Arrays.toString(fieldTypes.toArray()),
                    sb.toString()
            );
        }

        logTraceEvent(trace, headers, sinkDest);

        return parsedRowData;
    }


    enum TraceErrorCode {

        // 数据包含换行符：不应该算作错误，但是会跟之前text+lzo的双写校验出现偏差
        CONTAINS_NEWLINE_CHARACTER("contains newline character", "contains newline character"),

        // 最后一位不是分隔符
        END_WITHOUT_DELIMITER_KEY("end without delimiter key", "end without delimiter key"),

        // 字段多
        TOO_MANY_FIELDS("too many fields",  "too many fields"),

        // 类型转换错误
        PARSE_ERROR("parse error", "parse error"),

        //字段缺失
        MISS_FIELD("miss field", "miss field");

        private String errorCode;
        private String errorMsg;

        TraceErrorCode(String errorCode, String errorMsg) {
            this.errorCode = errorCode;
            this.errorMsg = errorMsg;
        }
    }
}
