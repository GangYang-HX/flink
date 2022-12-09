package org.apache.flink.table.runtime.operators.join.latency.compress.fastgzip;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2019/12/12
 * @Time:2:03 PM
 */
class ReusableGzipInputStream extends GZIPInputStream implements ReusableStream {

    private final static Logger LOG = LoggerFactory.getLogger(ReusableGzipInputStream.class);

    public ReusableGzipInputStream(int size) throws IOException {
        super(size);
    }

    public void setInputStream(InputStream in) {
        this.in = in;
        try {
            readHeader(this.in);
        } catch (IOException e) {
            LOG.error("setInputStream error", e);
        }
    }

    @Override
    public void reset() {
        inf.reset();
        crc.reset();
        eos = false;
        this.in = null;
    }
}
