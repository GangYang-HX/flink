package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 判断字符是否可以转化为数字
 * 
 * @author zhangyang
 * @Date:2019/10/23
 * @Time:3:15 PM
 */
public class IsNumber extends ScalarFunction{

    public Boolean eval(String key) {
        try {
            Long.valueOf(key);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.BOOLEAN;
    }
}
