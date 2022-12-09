package com.bilibili.bsql.mysql;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author zhangyang
 * @Date:2019/11/7
 * @Time:2:36 PM
 */
public class ToUnixtime extends ScalarFunction {

    public Long eval(String date, String format) {
        try {
            return SqlDateTimeUtils.toUnixtime(date, format);
        } catch (Exception e) {
            return 0L;
        }
    }

    public Long eval(Timestamp timestamp) {
        try {
            return timestamp.getTime();
        } catch (Exception e) {
            return 0L;
        }
    }
}
