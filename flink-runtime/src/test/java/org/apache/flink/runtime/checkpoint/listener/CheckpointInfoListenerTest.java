package org.apache.flink.runtime.checkpoint.listener;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class CheckpointInfoListenerTest {

	private static final CheckpointInfoListener listener = CheckpointInfoListenerUtil.getRegisteredListeners(true).get(0);

	private static final long checkpointId = 1L;

	@Test
	public void testNotify() throws IOException {
		TemporaryFolder temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
		final Path checkpointDir = new Path(temporaryFolder.newFolder().toURI());
		final FsCheckpointStorageLocation location = new FsCheckpointStorageLocation(
				LocalFileSystem.getSharedInstance(),
				checkpointDir, checkpointDir, checkpointDir,
				CheckpointStorageLocationReference.getDefault(),
				1024,
				4096);
		PendingCheckpoint pendingCheckpoint = new PendingCheckpoint(
				new JobID(),
				checkpointId,
				System.currentTimeMillis(),
				Collections.singletonMap(new ExecutionAttemptID(), Mockito.mock(ExecutionVertex.class)),
				Collections.emptyList(),
				Collections.emptyList(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				location,
				Executors.directExecutor(),
				new CompletableFuture<>());
		CheckpointInfoFactory factory = new CheckpointInfoFactory();
		CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(
				new JobID(),
				checkpointId,
				System.currentTimeMillis(),
				System.currentTimeMillis(),
				Collections.emptyMap(),
				Collections.emptyList(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				Mockito.mock(CompletedCheckpointStorageLocation.class)
		);
		listener.notifyCheckpointTriggered(factory.createTriggeredCheckpointInfo(pendingCheckpoint));
		listener.notifyCheckpointFailure(factory.createFailedCheckpointInfo(pendingCheckpoint,
				CheckpointFailureReason.TASK_CHECKPOINT_FAILURE), null);
		listener.notifyCheckpointComplete(factory.createCompletedCheckpointInfo(
				pendingCheckpoint,
				Collections.emptyMap(),
				System.currentTimeMillis(),
				false));
		listener.notifyCheckpointExpiration(factory.createExpiredCheckpointInfo(pendingCheckpoint));
		listener.notifyCheckpointDiscarded(factory.createDiscardedCheckpointInfo(completedCheckpoint));
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
					|| checkpointInfo.getResult() == CheckpointInfo.CheckpointResult.COMPLETED_REGIONAL;
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
