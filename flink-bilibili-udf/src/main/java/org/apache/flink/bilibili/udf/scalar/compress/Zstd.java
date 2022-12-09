package org.apache.flink.bilibili.udf.scalar.compress;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zhangyang
 * @Date:2022/3/22
 * @Time:11:28 上午
 */
public class Zstd extends ScalarFunction {
    public byte[] eval(String val){
        if (val == null) {
            return null;
        }
        return com.github.luben.zstd.Zstd.compress(val.getBytes());
    }

    public byte[] eval(byte[] val){
        if (val == null) {
            return null;
        }
        return com.github.luben.zstd.Zstd.compress(val);
    }
}
