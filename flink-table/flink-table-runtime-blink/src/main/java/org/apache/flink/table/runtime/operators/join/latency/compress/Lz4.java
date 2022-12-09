package org.apache.flink.table.runtime.operators.join.latency.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * @author zhangyang
 * @Date:2019/12/12
 * @Time:11:23 AM
 */

@NotThreadSafe
public class Lz4 extends Compress {

    private final static Logger   LOG         = LoggerFactory.getLogger(Lz4.class);
    private final static int      BUFFER_SIZE = 50 * 1024;
    private LZ4Compressor         lz4Compressor;
    private LZ4FastDecompressor   lz4Decompressor;
    private ByteArrayOutputStream byteArrayOutputStream;
    private byte[]                buffer      = new byte[BUFFER_SIZE];

    public Lz4(MetricGroup metricGroup) {
        super(metricGroup);
        byteArrayOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
        lz4Compressor = LZ4Factory.fastestInstance().highCompressor();
        lz4Decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public byte[] doCompress(byte[] bytes) {
        try {
            try (LZ4BlockOutputStream lz4Output = new LZ4BlockOutputStream(byteArrayOutputStream, 4096, lz4Compressor)) {
                lz4Output.write(bytes);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            LOG.error("compress error", e);
            return null;
        } finally {
            byteArrayOutputStream.reset();
        }
    }

    @Override
    public byte[] doUncompress(byte[] bytes) {
        int len;
        try (LZ4BlockInputStream lz4BlockInputStream = new LZ4BlockInputStream(new ByteArrayInputStream(bytes),
                                                                               lz4Decompressor)) {
            while ((len = lz4BlockInputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, len);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            LOG.error("uncompress error", e);
            return null;
        } finally {
            byteArrayOutputStream.reset();
        }
    }
}
