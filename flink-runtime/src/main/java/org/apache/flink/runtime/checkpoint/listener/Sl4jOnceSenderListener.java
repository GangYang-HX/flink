package org.apache.flink.runtime.checkpoint.listener;

import org.apache.flink.runtime.statistics.StatisticsUtils;
import org.apache.flink.runtime.statistics.metrics.Slf4jOnceSender;

import java.util.HashMap;
import java.util.Map;

/**
 *  Use {@link Slf4jOnceSender} to print checkpoint information
 *  and writing to ClickHouse for analysis.
 */
public class Sl4jOnceSenderListener
        implements CheckpointResultListener {

    // constant fields
    private static final String METRIC_NAME = "checkpointInfo";

    // common fields
    private static final String CHECKPOINT_ID = "checkpointId";
    private static final String CHECKPOINT_TRIGGER_TIME = "triggerTime";
    private static final String DURATION = "duration";
    private static final String CHECKPOINT_TYPE = "type";

    // completed
    private static final String COMPLETED_VALUE = "completed";
    private static final String COMPLETED_SIZE = "size";
    private static final String COMPLETED_IS_LOGICALLY_MAPPED = "isLogicallyMapped";

    // failed
    private static final String FAILED_VALUE = "failed";
    private static final String FAILED_CAUSE = "cause";


    @Override
    public void notifyCheckpointResult(CheckpointResult result) {
        Map<String, String> labels = new HashMap<>();
        labels.put(CHECKPOINT_ID, String.valueOf(result.getCheckpointId()));
        labels.put(CHECKPOINT_TRIGGER_TIME, String.valueOf(result.getTriggerTime()));
        labels.put(DURATION, String.valueOf(result.getDuration()));
        labels.put(CHECKPOINT_TYPE, result.getType().name());
        String metricValue;
        switch (result.getStatus()) {
            case COMPLETED:
                metricValue = COMPLETED_VALUE;
                labels.put(COMPLETED_SIZE, String.valueOf(result.getSize()));
                labels.put(COMPLETED_IS_LOGICALLY_MAPPED, String.valueOf(result.isLogicallyMapped()));
                break;
            case FAILED:
                metricValue = FAILED_VALUE;
                labels.put(FAILED_CAUSE, result.getMessage());
                break;
            default:
                throw new IllegalStateException("unknown status: " + result.getStatus());
        }
        StatisticsUtils.report(METRIC_NAME, metricValue, labels);
    }
}
