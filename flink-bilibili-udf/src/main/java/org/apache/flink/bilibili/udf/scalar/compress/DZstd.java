package org.apache.flink.bilibili.udf.scalar.compress;

import org.apache.flink.table.functions.ScalarFunction;

import com.github.luben.zstd.Zstd;

/**
 * @author zhangyang
 * @Date:2022/3/22
 * @Time:11:29 上午
 */
public class DZstd extends ScalarFunction {

    public String eval(byte[] val) {
        if (val == null) {
            return null;
        }
        int length = (int) Zstd.decompressedSize(val);
        byte[] content = Zstd.decompress(val, length);
        return new String(content);
    }
}
