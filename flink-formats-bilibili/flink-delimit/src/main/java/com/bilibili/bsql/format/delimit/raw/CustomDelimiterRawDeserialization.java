package com.bilibili.bsql.format.delimit.raw;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.parser.ByteParser;
import org.apache.flink.types.parser.DoubleParser;
import org.apache.flink.types.parser.FloatParser;
import org.apache.flink.types.parser.IntParser;
import org.apache.flink.types.parser.LongParser;
import org.apache.flink.types.parser.ShortParser;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import static com.bilibili.bsql.format.delimit.utils.DelimitStringUtils.unicodeStringDecode;

/** CustomDelimiterRawDeserialization. */
public class CustomDelimiterRawDeserialization extends AbstractDeserializationSchema<RowData> {

    private static final long serialVersionUID = 2385115670960445646L;

    private static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();

    private static final DateTimeFormatter DATA_FORMAT_20 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssz");
    private static final DateTimeFormatter DATA_FORMAT_24 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");
    private static final DateTimeFormatter DATA_FORMAT_21 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final String MILLISECONDS_PADDING = "000";

    //	private final String[] fieldNames;
    //    protected final TypeInformation<?>[] fieldTypes;
    protected int totalFieldsLength;
    protected final byte delimiterKey;
    protected final RowType rowType;
    public boolean hasSomeUnexpected;
    protected int currentFieldStartPosition;
    protected int currentFieldIndexInRowData;

    protected transient RawDeserializationConverter[] toInternalConverters;
    protected transient Object[] toInternalDefault;

    protected transient Counter abnormalInputCounter;

    public CustomDelimiterRawDeserialization(
            TypeInformation<RowData> typeInfo, RowType rowType, String delimiterKey) {
        this.rowType = rowType;
        this.totalFieldsLength = rowType.getFieldCount();
        String delimitString = unicodeStringDecode(delimiterKey);

        byte[] delimitInput;
        try {
            delimitInput = delimitString.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (delimitInput.length != 1) {
            throw new RuntimeException("source format delimit key should only have one character");
        }
        this.delimiterKey = delimitInput[0];
    }

    private void initFieldParser() {
        toInternalConverters = new RawDeserializationConverter[rowType.getFieldCount()];
        toInternalDefault = new Object[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType type = rowType.getTypeAt(i);

            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    toInternalConverters[i] = BinaryStringData::new;
                    toInternalDefault[i] = StringData.fromString("");
                    break;
                case BIGINT:
                case INTERVAL_DAY_TIME:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) ->
                                    LongParser.parseField(
                                            segments[0].getArray(), offset, sizeInBytes);
                    toInternalDefault[i] = 0L;
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) -> {
                                BinaryStringData timeString =
                                        new BinaryStringData(segments, offset, sizeInBytes);
                                return convertToTimestamp(timeString.toString());
                            };
                    toInternalDefault[i] = TimestampData.fromEpochMillis(0);
                    break;
                case BOOLEAN:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) ->
                                    "true"
                                            .equalsIgnoreCase(
                                                    new BinaryStringData(
                                                                    segments, offset, sizeInBytes)
                                                            .toString());
                    toInternalDefault[i] = false;
                    break;
                case INTEGER:
                case INTERVAL_YEAR_MONTH:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) ->
                                    IntParser.parseField(
                                            segments[0].getArray(), offset, sizeInBytes);
                    toInternalDefault[i] = 0;
                    break;
                case TINYINT:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) ->
                                    ByteParser.parseField(
                                            segments[0].getArray(), offset, sizeInBytes);
                    toInternalDefault[i] = (byte) 0;
                    break;
                case SMALLINT:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) ->
                                    ShortParser.parseField(
                                            segments[0].getArray(), offset, sizeInBytes);
                    toInternalDefault[i] = (short) 0;
                    break;
                case FLOAT:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) -> {
                                if (sizeInBytes == 0) {
                                    return (float) 0;
                                }
                                return FloatParser.parseField(
                                        segments[0].getArray(), offset, sizeInBytes);
                            };

                    toInternalDefault[i] = (float) 0;
                    break;
                case DOUBLE:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) -> {
                                if (sizeInBytes == 0) {
                                    return (double) 0;
                                }
                                return DoubleParser.parseField(
                                        segments[0].getArray(), offset, sizeInBytes);
                            };
                    toInternalDefault[i] = (double) 0;
                    break;
                case BINARY:
                case VARBINARY:
                    toInternalConverters[i] =
                            (segments, offset, sizeInBytes) -> {
                                byte[] result = new byte[sizeInBytes];
                                System.arraycopy(
                                        segments[0].getArray(), offset, result, 0, sizeInBytes);
                                return result;
                            };
                    toInternalDefault[i] = new byte[0];
                    break;
                default:
                    throw new UnsupportedOperationException(
                            type.toString() + " is not supported in raw source format");
            }
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        if (context != null) {
            abnormalInputCounter = context.getMetricGroup().counter("source_abnormal_input");
        }
        hasSomeUnexpected = false;
        currentFieldStartPosition = 0;
        currentFieldIndexInRowData = 0;
        initFieldParser();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {

        hasSomeUnexpected = false;
        currentFieldStartPosition = 0;
        currentFieldIndexInRowData = 0;

        GenericRowData parsedRowData = new GenericRowData(totalFieldsLength);

        MemorySegment[] memSegment = new MemorySegment[] {MemorySegmentFactory.wrap(message)};

        for (int position = 0; position < message.length; position++) {
            if (currentFieldIndexInRowData >= rowType.getFieldCount()) {
                /*
                 * if the record has something not in the schema, just abrupt the deser process
                 * */
                hasSomeUnexpected = true;
                break;
            }

            byte current = message[position];
            if (current == delimiterKey) {
                Object currentFieldObject;
                try {
                    currentFieldObject =
                            toInternalConverters[currentFieldIndexInRowData].deserialize(
                                    memSegment,
                                    currentFieldStartPosition,
                                    position - currentFieldStartPosition);
                } catch (Exception parseExp) {
                    hasSomeUnexpected = true;
                    currentFieldObject = toInternalDefault[currentFieldIndexInRowData];
                }

                parsedRowData.setField(currentFieldIndexInRowData++, currentFieldObject);
                currentFieldStartPosition = position + 1;
                continue;
            }

            if (position == message.length - 1) {
                Object currentFieldObject;
                try {
                    currentFieldObject =
                            toInternalConverters[currentFieldIndexInRowData].deserialize(
                                    memSegment,
                                    currentFieldStartPosition,
                                    position + 1 - currentFieldStartPosition);
                } catch (Exception parseExp) {
                    hasSomeUnexpected = true;
                    currentFieldObject = toInternalDefault[currentFieldIndexInRowData];
                }

                parsedRowData.setField(currentFieldIndexInRowData++, currentFieldObject);
                break;
            }
        }

        /*
         * fill in the remaining field if the data is less than expected
         * if field count match, two value should match
         * */
        if (currentFieldIndexInRowData < rowType.getFieldCount()) {
            for (int i = currentFieldIndexInRowData; i < rowType.getFieldCount(); i++) {
                parsedRowData.setField(i, toInternalDefault[i]);
            }

            /*
             * if end with a delimit key, the last value is just empty, should not count as failure
             * */
            if (message[message.length - 1] != delimiterKey) {
                hasSomeUnexpected = true;
            }
        }

        if (hasSomeUnexpected && abnormalInputCounter != null) {
            abnormalInputCounter.inc();
        }

        return parsedRowData;
    }

    @FunctionalInterface
    interface RawDeserializationConverter extends Serializable {
        Object deserialize(MemorySegment[] segments, int offset, int sizeInBytes);
    }

    public static TimestampData convertToTimestamp(String timeStampStr) {
        if (StringUtils.isNumeric(timeStampStr)) {
            if (timeStampStr.length() == 10) {
                timeStampStr += MILLISECONDS_PADDING;
            }
            return TimestampData.fromEpochMillis(
                    new Timestamp(Long.parseLong(timeStampStr)).getTime());
        } else if (timeStampStr.contains("T")) {
            if (timeStampStr.length() == 20) {
                return TimestampData.fromEpochMillis(formatByPattern(timeStampStr, DATA_FORMAT_20));
            } else if (timeStampStr.length() == 21 || timeStampStr.length() == 19) {
                return TimestampData.fromEpochMillis(formatByPattern(timeStampStr, DATA_FORMAT_21));
            } else if (timeStampStr.length() == 24) {
                return TimestampData.fromEpochMillis(formatByPattern(timeStampStr, DATA_FORMAT_24));
            } else {
                throw new RuntimeException(
                        String.format("Can not format timestamp %s", timeStampStr));
            }
        } else {
            return TimestampData.fromEpochMillis(Timestamp.valueOf(timeStampStr).getTime());
        }
    }

    private static long formatByPattern(String timestamp, DateTimeFormatter dateTimeFormatter) {
        // Temporal values with UTC, need to plus 8 hour
        // https://github.com/vert-x3/vertx-jdbc-client/issues/20
        return LocalDateTime.from(dateTimeFormatter.parse(timestamp))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }
}
