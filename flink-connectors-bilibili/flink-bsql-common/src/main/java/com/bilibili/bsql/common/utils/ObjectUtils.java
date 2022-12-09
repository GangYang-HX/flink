package com.bilibili.bsql.common.utils;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

/** ObjectUtils. */
public class ObjectUtils {

    public static Object getDefaultValue(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString("");
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return new Long(0);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // when play back from checkpoint, if a 'now' value appear, can push the window
                // forward
                // which will cause many record dropped
                return TimestampData.fromEpochMillis(0);
            case BOOLEAN:
                return new Boolean(false);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return new Integer(0);
            case TINYINT:
                return new Byte((byte) 0);
            case SMALLINT:
                return new Short((short) 0);
            case FLOAT:
                return new Float(0);
            case DOUBLE:
                return new Double(0);
        }
        return "";
    }
}
