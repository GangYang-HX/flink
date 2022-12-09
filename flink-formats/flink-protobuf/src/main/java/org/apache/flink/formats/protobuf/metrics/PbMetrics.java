package org.apache.flink.formats.protobuf.metrics;

/** PbMetrics. */
public interface PbMetrics extends Metrics {

    void dirtyData();

    void defaultValue();
}
