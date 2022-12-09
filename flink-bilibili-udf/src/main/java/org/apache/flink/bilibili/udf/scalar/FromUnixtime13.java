package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.bilibili.udf.utils.SqlDateTimeUtils;
import org.apache.flink.table.functions.ScalarFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间戳转日期
 * 
 * @author zhangyang
 * @Date:2019/10/22
 * @Time:11:33 PM
 */

public class FromUnixtime13 extends ScalarFunction {

    private final static Logger LOG = LoggerFactory.getLogger(FromUnixtime13.class);

    public String eval(Long timestamp, String format) {
        try {
            return SqlDateTimeUtils.fromUnixtime(timestamp, format);
        } catch (Exception e) {
            return "";
        }
    }

    public String eval(String timestamp, String format) {
        try {
            return SqlDateTimeUtils.fromUnixtime(Long.valueOf(timestamp.trim()), format);
        } catch (Exception e) {
            return "";
        }
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }

}
