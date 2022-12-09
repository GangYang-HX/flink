package org.apache.flink.bilibili.udf.scalar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zhangyang
 * @Date:2019/11/8
 * @Time:5:09 PM
 */
public class Length extends ScalarFunction {

    public int eval(String value) {
        if (StringUtils.isEmpty(value)) {
            return 0;
        }
        return value.length();
    }
}
