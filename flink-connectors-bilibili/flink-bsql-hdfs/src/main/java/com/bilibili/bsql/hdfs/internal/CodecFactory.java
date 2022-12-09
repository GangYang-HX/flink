package com.bilibili.bsql.hdfs.internal;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author zhangyang
 * @Date:2020/5/19
 * @Time:6:46 PM
 */
public class CodecFactory {

    private final static String LZO = ".lzo";

    public static InputStream wrap(String fileName, FSDataInputStream in) throws IOException {
        if (fileName.endsWith(LZO)) {
            LzopCodec codec = new LzopCodec();
            codec.setConf(new Configuration());
            return codec.createInputStream(in);
        }
        return in;
    }
}
