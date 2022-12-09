package org.apache.flink.table.runtime.operators.join.latency.compress.fastgzip;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2019/12/12
 * @Time:2:04 PM
 */
class ReusableGzipOutputStream extends GZIPOutputStream implements ReusableStream {

    private final static Logger LOG = LoggerFactory.getLogger(ReusableGzipOutputStream.class);

    public ReusableGzipOutputStream(OutputStream out, int size, boolean syncFlush) throws IOException {
        super(out, size, syncFlush);
    }

    @Override
    public void reset() {
        try {
            def.reset();
            writeHeader();
            crc.reset();
        } catch (IOException e) {
            LOG.error("reset ReusableGzipOutputStream error", e);
        }

    }
}
