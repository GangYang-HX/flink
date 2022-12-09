package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.listener.CheckpointInfo;
import org.apache.flink.runtime.checkpoint.listener.CheckpointInfoListener;

/**
 * This is an empty implementation of {@link CheckpointInfoListener}. This is supplied because the
 * users may only care about one or two of the notifications. We supply this empty implementation so
 * that they don't have to implement the unnecessary methods.
 */
public abstract class AbstractCheckpointInfoListener implements CheckpointInfoListener {
    @Override
    public void notifyCheckpointTriggered(CheckpointInfo checkpointInfo) {}

    @Override
    public void notifyCheckpointComplete(CheckpointInfo checkpointInfo) {}

    @Override
    public void notifyCheckpointFailure(CheckpointInfo checkpointInfo, Throwable reason) {}

    @Override
    public void notifyCheckpointExpiration(CheckpointInfo checkpointInfo) {}

    @Override
    public void notifyCheckpointDiscarded(CheckpointInfo checkpointInfo) {}
}
