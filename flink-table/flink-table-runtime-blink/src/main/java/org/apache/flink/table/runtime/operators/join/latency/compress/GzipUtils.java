/**
 * Bilibili.com Inc. Copyright (c) 2009-2018 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GZIP 解压缩工具类
 *
 * @author Xinux
 */
public class GzipUtils {

    private static final Logger LOG         = LoggerFactory.getLogger(GzipUtils.class);
    private static final int    BUFFER_SIZE = 1024;

    /**
     * 字符串压缩为GZIP字节数组
     *
     * @param bytes
     * @return
     */
    public static byte[] compress(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(bytes);
            gzip.finish();
        } catch (IOException e) {
            LOG.error("gzip compress error", e);
        }
        return out.toByteArray();
    }

    /**
     * GZIP解压缩
     *
     * @param bytes
     * @return
     */
    public static byte[] uncompress(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(in)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = gzipInputStream.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        } catch (Exception e) {
            LOG.error("gzip uncompress error:", e);
        }
        return null;
    }
}
