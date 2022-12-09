package org.apache.flink.bilibili.udf.scalar.hash;

import org.apache.flink.bilibili.udf.utils.MurmurHashUtils;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zhangyang
 * @Date:2022/3/22
 * @Time:11:38 上午
 */
public class Murmur64 extends ScalarFunction {
    public long eval(String val) {
        if (val == null) {
            return 0;
        }

        long ret = MurmurHashUtils.hash64(val.getBytes());

        //取绝对值,但是Long.MIN_VALUE取绝对值后会溢出,直接置0
        if (ret == Long.MIN_VALUE) {
            return 0;
        }
        return Math.abs(ret);
    }
}
