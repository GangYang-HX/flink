package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.bilibili.udf.utils.SqlDateTimeUtils;
import org.apache.flink.table.functions.ScalarFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2019/11/1
 * @Time:10:25 AM
 */
public class DateReFormat extends ScalarFunction {

    private final static Logger LOG = LoggerFactory.getLogger(DateReFormat.class);

    public String eval(String date, String format, String newFormat) {
        try {
            return SqlDateTimeUtils.dataReformat(date, format, newFormat);
        } catch (Exception e) {
            return date;
        }
    }
}
