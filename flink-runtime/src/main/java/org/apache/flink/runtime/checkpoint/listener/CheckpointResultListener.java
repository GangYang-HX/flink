package org.apache.flink.runtime.checkpoint.listener;

public interface CheckpointResultListener {

    void notifyCheckpointResult(CheckpointResult result);

}
