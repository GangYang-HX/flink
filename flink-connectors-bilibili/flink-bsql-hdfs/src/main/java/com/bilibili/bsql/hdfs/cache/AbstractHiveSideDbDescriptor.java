package com.bilibili.bsql.hdfs.cache;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.TypeDescription;

/**
 * @Author: JinZhengyu
 * @Date: 2022/8/3 下午9:12
 */
public abstract class AbstractHiveSideDbDescriptor {


    public static Object convert(ColumnVector vector, TypeDescription schema, int row) {
        if (vector.isRepeating) {
            row = 0;
        }
        if (!vector.noNulls && vector.isNull[row]) {
            return null;
        } else {
            switch (schema.getCategory()) {
                case BOOLEAN:
                    return ((LongColumnVector) vector).vector[row] != 0L;
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                    return ((LongColumnVector) vector).vector[row];
                case FLOAT:
                case DOUBLE:
                    return ((DoubleColumnVector) vector).vector[row];
                case STRING:
                case CHAR:
                case VARCHAR:
                    return ((BytesColumnVector) vector).toString(row);
                case DECIMAL:
                    return ((DecimalColumnVector) vector).vector[row].toString();
                case DATE:
                    return (new DateWritable((int) ((LongColumnVector) vector).vector[row])).toString();
                case TIMESTAMP:
                case TIMESTAMP_INSTANT:
                    return ((TimestampColumnVector) vector).asScratchTimestamp(row).toString();
                case BINARY:
                case LIST:
                case MAP:
                case STRUCT:
                case UNION:
                    throw new IllegalArgumentException("Unsupported type " + schema.toString());
                default:
                    throw new IllegalArgumentException("Unknown type " + schema.toString());
            }
        }
    }


}
