package org.apache.flink.runtime.checkpoint.listener;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpointTest;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;

public class CheckpointInfoListenerTest {

    private static final List<Execution> ACK_TASKS = new ArrayList<>();

    private static final CheckpointInfoListener listener =
            CheckpointInfoListenerUtil.getRegisteredListeners(true).get(0);

    private static final long checkpointId = 1L;

    static {
        Execution execution = mock(Execution.class);
        ACK_TASKS.add(execution);
    }

    @Test
    public void testNotify() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final Path checkpointDir = new Path(temporaryFolder.newFolder().toURI());
        final FsCheckpointStorageLocation location =
                new FsCheckpointStorageLocation(
                        LocalFileSystem.getSharedInstance(),
                        checkpointDir,
                        checkpointDir,
                        checkpointDir,
                        CheckpointStorageLocationReference.getDefault(),
                        1024,
                        4096);
        PendingCheckpointTest.RecordCheckpointPlan recordCheckpointPlan =
                new PendingCheckpointTest.RecordCheckpointPlan(new ArrayList<>(ACK_TASKS));
        PendingCheckpoint pendingCheckpoint =
                new PendingCheckpoint(
                        new JobID(),
                        checkpointId,
                        System.currentTimeMillis(),
                        recordCheckpointPlan,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new CompletableFuture<>(),
                        null);
        pendingCheckpoint.setCheckpointTargetLocation(location);
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        checkpointId,
                        System.currentTimeMillis(),
                        System.currentTimeMillis(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        Mockito.mock(CompletedCheckpointStorageLocation.class),
                        null);
        CheckpointInfoFactory factory = new CheckpointInfoFactory();
        listener.notifyCheckpointTriggered(
                factory.createTriggeredCheckpointInfo(pendingCheckpoint));
        listener.notifyCheckpointFailure(
                factory.createFailedCheckpointInfo(pendingCheckpoint, null), null);
        listener.notifyCheckpointComplete(
                factory.createCompletedCheckpointInfo(
                        pendingCheckpoint,
                        Collections.emptyMap(),
                        System.currentTimeMillis(),
                        false));
        listener.notifyCheckpointExpiration(factory.createExpiredCheckpointInfo(pendingCheckpoint));
        listener.notifyCheckpointDiscarded(
                factory.createDiscardedCheckpointInfo(completedCheckpoint));
    }

    public static class TestCheckpointInfoListener implements CheckpointInfoListener {

        @Override
        public void notifyCheckpointTriggered(CheckpointInfo checkpointInfo) {
            assert checkpointInfo.getCheckpointId() == checkpointId;
        }

        @Override
        public void notifyCheckpointComplete(CheckpointInfo checkpointInfo) {
            assert checkpointInfo.getCheckpointId() == checkpointId;
            assert checkpointInfo.getResult() == CheckpointInfo.CheckpointResult.COMPLETED_GLOBAL
                    || checkpointInfo.getResult()
                            == CheckpointInfo.CheckpointResult.COMPLETED_REGIONAL;
        }

        @Override
        public void notifyCheckpointFailure(CheckpointInfo checkpointInfo, Throwable reason) {
            assert checkpointInfo.getCheckpointId() == checkpointId;
            assert checkpointInfo.getResult() == CheckpointInfo.CheckpointResult.FAILED;
        }

        @Override
        public void notifyCheckpointExpiration(CheckpointInfo checkpointInfo) {
            assert checkpointInfo.getCheckpointId() == checkpointId;
            assert checkpointInfo.getResult() == CheckpointInfo.CheckpointResult.CANCELED;
        }

        @Override
        public void notifyCheckpointDiscarded(CheckpointInfo checkpointInfo) {
            assert checkpointInfo.getCheckpointId() == checkpointId;
            assert checkpointInfo.getResult() == CheckpointInfo.CheckpointResult.DISCARDED;
        }
    }
}
