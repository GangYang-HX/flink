package org.apache.flink.table.runtime.operators.join.latency.compress;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.compress.fastgzip.FastGzip;


/**
 * @author zhangyang
 * @Date:2019/12/12
 * @Time:5:10 PM
 */
public final class CompressFactory {

    private CompressFactory() {

    }

    public static Compress produce(int compressType, MetricGroup metricGroup) {
        Compress compress;
        if (compressType == CompressEnums.GZIP.getCode()) {
            compress = new Gzip(metricGroup);
        } else if (compressType == CompressEnums.SNAPPY.getCode()) {
            compress = new Snappy(metricGroup);
        } else if (compressType == CompressEnums.FAST_GZIP.getCode()) {
            compress = new FastGzip(metricGroup);
        } else if (compressType == CompressEnums.LZ4.getCode()) {
            compress = new Lz4(metricGroup);
        } else {
            compress = new NoneCompress(metricGroup);
        }
        return compress;
    }
}
