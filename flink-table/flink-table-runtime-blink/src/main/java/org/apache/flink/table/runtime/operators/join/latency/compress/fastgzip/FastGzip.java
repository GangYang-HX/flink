package org.apache.flink.table.runtime.operators.join.latency.compress.fastgzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.compress.Compress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 重写了下jdk gzip的gzipInputStream和gzipOutputStream,调整了下压缩等级和复用了所有需要开辟新内存的byte
 *
 * @author zhangyang
 * @Date:2019/12/12
 * @Time:11:23 AM
 */

@NotThreadSafe
public class FastGzip extends Compress {

    private final static Logger      LOG         = LoggerFactory.getLogger(FastGzip.class);
    private final static int         BUFFER_SIZE = 50 * 1024;
    private ReusableGzipOutputStream gzipOutputStream;
    private ByteArrayOutputStream    byteArrayOutputStream;
    private ReusableGzipInputStream  gzipInputStream;
    private byte[]                   buffer      = new byte[BUFFER_SIZE];
    private ByteArrayInputStream     byteArrayInputStream;

    public FastGzip(MetricGroup metricGroup) {
        super(metricGroup);
        try {
            byteArrayOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
            gzipOutputStream = new ReusableGzipOutputStream(byteArrayOutputStream, BUFFER_SIZE, false);
            gzipInputStream = new ReusableGzipInputStream(BUFFER_SIZE);
        } catch (IOException e) {
            LOG.error("int gzip2Inner error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] doCompress(byte[] bytes) {
        try {
            byteArrayOutputStream.reset();
            gzipOutputStream.reset();

            gzipOutputStream.write(bytes);
            gzipOutputStream.finish();
            byte[] rs = byteArrayOutputStream.toByteArray();
            return rs;
        } catch (IOException e) {
            LOG.error("compress error", e);
            return null;
        }
    }

    @Override
    public byte[] doUncompress(byte[] bytes) {
        try {
            gzipInputStream.reset();
            byteArrayOutputStream.reset();

            byteArrayInputStream = new ByteArrayInputStream(bytes);
            gzipInputStream.setInputStream(byteArrayInputStream);
            int n;
            while ((n = gzipInputStream.read(buffer)) >= 0) {
                byteArrayOutputStream.write(buffer, 0, n);
            }
            byteArrayInputStream.close();
            byte[] rs = byteArrayOutputStream.toByteArray();
            return rs;
        } catch (IOException e) {
            LOG.error("uncompress error", e);
            return null;
        }
    }
}
