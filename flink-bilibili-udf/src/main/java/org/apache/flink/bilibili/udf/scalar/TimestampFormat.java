package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.bilibili.udf.utils.SqlDateTimeUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author zhangyang
 * @Date:2019/11/8
 * @Time:10:37 PM
 */
public class TimestampFormat extends ScalarFunction {

    public String eval(Timestamp timestamp, String format) {
        try {
            return SqlDateTimeUtils.fromUnixtime(timestamp.getTime(), format);
        } catch (Exception e) {
            return "";
        }

    }
}
