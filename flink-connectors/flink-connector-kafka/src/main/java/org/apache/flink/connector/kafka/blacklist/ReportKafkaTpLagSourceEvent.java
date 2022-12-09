package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.connector.source.SourceEvent;

import java.io.Serializable;
import java.util.Properties;

/** SourceEvent send from Kafka source reader to KafkaSourceEnumerator. */
public class ReportKafkaTpLagSourceEvent implements SourceEvent, Serializable {

    private final Integer subtaskId;

    private final Properties blacklistMetric;

    public ReportKafkaTpLagSourceEvent(Properties blacklistMetric, Integer subtaskId) {
        this.subtaskId = subtaskId;
        this.blacklistMetric = blacklistMetric;
    }

    public Integer getSubtaskId() {
        return subtaskId;
    }

    public Properties getBlacklistMetric() {
        return blacklistMetric;
    }
}
