package org.apache.flink.runtime.checkpoint.listener;

import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;

public class CheckpointResult {

    private long checkpointId;
    private long triggerTime;
    private CheckpointType type;
    private CheckpointFinalStatus status;
    private long duration;
    private boolean isLogicallyMapped;
    private long size;
    private String message;


    public long getCheckpointId() {
        return checkpointId;
    }

    public CheckpointResult setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
        return this;
    }

    public long getTriggerTime() {
        return triggerTime;
    }

    public CheckpointResult setTriggerTime(long triggerTime) {
        this.triggerTime = triggerTime;
        return this;
    }

    public CheckpointFinalStatus getStatus() {
        return status;
    }

    public CheckpointResult setStatus(CheckpointFinalStatus status) {
        this.status = status;
        return this;
    }

    public long getDuration() {
        return duration;
    }

    public CheckpointResult setDuration(long duration) {
        this.duration = duration;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public CheckpointResult setMessage(String message) {
        this.message = message;
        return this;
    }

    public long getSize() {
        return size;
    }

    public CheckpointResult setSize(long size) {
        this.size = size;
        return this;
    }

    public CheckpointType getType() {
        return type;
    }

    public CheckpointResult setType(CheckpointType type) {
        this.type = type;
        return this;
    }

    public boolean isLogicallyMapped() {
        return isLogicallyMapped;
    }

    public CheckpointResult setLogicallyMapped(boolean logicallyMapped) {
        isLogicallyMapped = logicallyMapped;
        return this;
    }

    public static CheckpointResult createFromCheckpointStats(AbstractCheckpointStats stats) {
        CheckpointResult result = new CheckpointResult().setCheckpointId(stats.getCheckpointId())
                .setTriggerTime(stats.getTriggerTimestamp())
                .setDuration(stats.getEndToEndDuration())
                .setType(stats.getProperties().getCheckpointType());
        if (stats instanceof CompletedCheckpointStats) {
            result.setStatus(CheckpointFinalStatus.COMPLETED);
            result.setLogicallyMapped(stats.getNumberOfLogicallyMappedSubtasks() > 0);
            result.setSize(stats.getStateSize());
        } else if (stats instanceof FailedCheckpointStats) {
            FailedCheckpointStats failedCheckpointStats = (FailedCheckpointStats) stats;
            result.setStatus(CheckpointFinalStatus.FAILED);
            result.setMessage(failedCheckpointStats.getFailureMessage());
        } else {
            result.setStatus(CheckpointFinalStatus.UNKNOW);
        }
        return result;
    }

    enum CheckpointFinalStatus {
        COMPLETED,
        FAILED,
        UNKNOW
    }
}

