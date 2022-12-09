package org.apache.flink.bilibili.udf.scalar.compress;

import com.github.luben.zstd.Zstd;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zhangyang
 * @Date:2022/3/22
 * @Time:11:29 上午
 */
public class DZstdByte extends ScalarFunction {

    public byte[] eval(byte[] val) {
        if (val == null) {
            return null;
        }
        int length = (int) Zstd.decompressedSize(val);
        return Zstd.decompress(val, length);
    }
}
