package org.apache.flink.taishan.state.snapshot;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.taishan.state.bloomfilter.BloomFilterStore;
import org.apache.flink.taishan.state.client.TaishanAdminConfiguration;
import org.apache.flink.taishan.state.restore.TaishanRestoreResult;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TaishanSnapshotStrategy<K>
	extends AbstractSnapshotStrategy<KeyedStateHandle>
	implements CheckpointListener {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanSnapshotStrategy.class);

	/** A {@link CloseableRegistry} that will be closed when the task is cancelled. */
	@Nonnull
	protected final CloseableRegistry cancelStreamRegistry;
	protected final TypeSerializer<K> keySerializer;
	protected final LocalRecoveryConfig localRecoveryConfig;
	protected final KeyGroupRange keyGroupRange;
	protected final StreamCompressionDecorator keyGroupCompressionDecorator;
	protected final TaishanRestoreResult restoreResult;
	protected final LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation;
	protected final TaishanAdminConfiguration taishanAdminConfiguration;
	private final BloomFilterStore bloomFilterStore;

	public TaishanSnapshotStrategy(
		TypeSerializer<K> keySerializer,
		LocalRecoveryConfig localRecoveryConfig,
		KeyGroupRange keyGroupRange,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		CloseableRegistry cancelStreamRegistry,
		TaishanRestoreResult restoreResult,
		LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation,
		BloomFilterStore bloomFilterStore,
		TaishanAdminConfiguration taishanAdminConfiguration) {
		super("Taishan backend snapshot");
		this.keySerializer = keySerializer;
		this.keyGroupRange = keyGroupRange;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.localRecoveryConfig = localRecoveryConfig;
		this.restoreResult = restoreResult;
		this.kvStateInformation = kvStateInformation;
		this.bloomFilterStore = bloomFilterStore;
		this.taishanAdminConfiguration = taishanAdminConfiguration;
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =
			createCheckpointStreamSupplier(checkpointId, streamFactory, checkpointOptions);

		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
		for (RegisteredStateMetaInfoBase stateInfo : kvStateInformation.values()) {
			// snapshot meta info
			stateMetaInfoSnapshots.add(stateInfo.snapshot());
		}

		String taishanSnapshotId = createTaishanSnapshot(checkpointId);
		checkNotNull(taishanSnapshotId == null, "create checkpoint failed.");

		TaishanSnapshotOperation taishanSnapshotOperation =
			new TaishanSnapshotOperation(checkpointStreamSupplier, streamFactory, checkpointId, stateMetaInfoSnapshots, taishanSnapshotId);

		return taishanSnapshotOperation.toAsyncSnapshotFutureTask(cancelStreamRegistry);
	}

	private String createTaishanSnapshot(long checkpointId) {
		String taishanSnapshotId = getTaishanSnapshotId(checkpointId);

		if (restoreResult.getTaishanClientWrapper() != null) {
			for (int keyGroup = keyGroupRange.getStartKeyGroup(); keyGroup <= keyGroupRange.getEndKeyGroup(); keyGroup++) {
				LOG.debug("create taishan snapshotId:{}, table:{}, keyGroup:{}", taishanSnapshotId, restoreResult.getTaishanTableName(), keyGroup);
				restoreResult.getTaishanClientWrapper().createCheckpoint(keyGroup, taishanSnapshotId);
			}
		} else {
			LOG.info("There is no keyed state.");
		}
		return taishanSnapshotId;
	}

	private SupplierWithException<CheckpointStreamWithResultProvider, Exception> createCheckpointStreamSupplier(
		long checkpointId,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions) {

		return localRecoveryConfig.isLocalRecoveryEnabled() && !checkpointOptions.getCheckpointType().isSavepoint() ?

			() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
				checkpointId,
				CheckpointedStateScope.EXCLUSIVE,
				primaryStreamFactory,
				localRecoveryConfig.getLocalStateDirectoryProvider()) :

			() -> CheckpointStreamWithResultProvider.createSimpleStream(
				CheckpointedStateScope.EXCLUSIVE,
				primaryStreamFactory);
	}

	private long lastCompletedCheckpointId = -1;

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (checkpointId > lastCompletedCheckpointId) {
			if (lastCompletedCheckpointId > 0) {
				// drop last checkpoint
				String taishanSnapshotId = getTaishanSnapshotId(lastCompletedCheckpointId);
				dropTaishanCheckpoint(taishanSnapshotId);
			}
			lastCompletedCheckpointId = checkpointId;
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		String taishanSnapshotId = getTaishanSnapshotId(checkpointId);
		dropTaishanCheckpoint(taishanSnapshotId);
	}

	private void dropTaishanCheckpoint(String taishanSnapshotId) {
		List<Integer> checkpointFailedList = null;
		for (int keyGroup = keyGroupRange.getStartKeyGroup(); keyGroup <= keyGroupRange.getEndKeyGroup(); keyGroup++) {
			try {
				restoreResult.getTaishanClientWrapper().dropCheckpoint(keyGroup, taishanSnapshotId);
			} catch (Exception e) {
				if (checkpointFailedList == null) {
					checkpointFailedList = new ArrayList<>();
				}
				checkpointFailedList.add(Integer.valueOf(keyGroup));
			}
		}
		if (checkpointFailedList != null) {
			LOG.warn("drop taishan checkpoint failed, tableName:{}, subtask:{}, snapshotId:{}, keyGroupRange:{}, failed keyGroup:{}",
				this.restoreResult.getTaishanTableName(), subTaskIndex, taishanSnapshotId, keyGroupRange, checkpointFailedList);
		}
	}

	private String getTaishanSnapshotId(long checkpointId) {
		return String.valueOf(checkpointId);
	}

	private final class TaishanSnapshotOperation
		extends AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> {
		private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;
		private CheckpointStreamFactory streamFactory;
		private long checkpointId;
		private final String taishanSnapshotId;
		private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;


		private TaishanSnapshotOperation(SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
										 CheckpointStreamFactory streamFactory,
										 long checkpointId,
										 List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
										 String taishanSnapshotId) {
			this.checkpointStreamSupplier = checkpointStreamSupplier;
			this.streamFactory = streamFactory;
			this.checkpointId = checkpointId;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			this.taishanSnapshotId = taishanSnapshotId;
		}

		@Override
		protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {
			final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider =
				checkpointStreamSupplier.get();

			snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
			writeSnapshotToOutputStream(checkpointStreamWithResultProvider);

			if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
				SnapshotResult<StreamStateHandle> snapshotResult = checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult();

				StreamStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();

				if (jobManagerOwnedSnapshot != null && restoreResult.getTaishanClientWrapper() != null) {
					KeyedStateHandle jmKeyedState = new TaishanKeyedStateHandle(keyGroupRange,
						jobManagerOwnedSnapshot,
						restoreResult.getTaishanTableName(),
						restoreResult.getAccessToken(),
						checkpointId,
						taishanSnapshotId,
						taishanAdminConfiguration.getCluster(),
						taishanAdminConfiguration.getGroupToken(),
						taishanAdminConfiguration.getGroupName(),
						taishanAdminConfiguration.getAppName());
					StreamStateHandle taskLocalSnapshot = snapshotResult.getTaskLocalSnapshot();

					if (taskLocalSnapshot != null) {
						KeyedStateHandle localKeyedState = new TaishanKeyedStateHandle(keyGroupRange,
							taskLocalSnapshot,
							restoreResult.getTaishanTableName(),
							restoreResult.getAccessToken(),
							checkpointId,
							taishanSnapshotId,
							taishanAdminConfiguration.getCluster(),
							taishanAdminConfiguration.getGroupToken(),
							taishanAdminConfiguration.getGroupName(),
							taishanAdminConfiguration.getAppName());
						return SnapshotResult.withLocalState(jmKeyedState, localKeyedState);
					} else {

						return SnapshotResult.of(jmKeyedState);
					}
				} else {
					return SnapshotResult.empty();
				}
			} else {
				throw new IOException("Stream is already unregistered/closed.");
			}
		}

		private void writeSnapshotToOutputStream(CheckpointStreamWithResultProvider checkpointStreamWithResultProvider) throws IOException {

			snapshotStateDescriptor(streamFactory, keySerializer);

			final DataOutputView outputView =
				new DataOutputViewStreamWrapper(checkpointStreamWithResultProvider.getCheckpointOutputStream());

			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(
					// TODO: this code assumes that writing a serializer is threadsafe, we should support to
					// get a serialized form already at state registration time in the future
					keySerializer,
					stateMetaInfoSnapshots,
					!Objects.equals(
						UncompressedStreamCompressionDecorator.INSTANCE,
						keyGroupCompressionDecorator));

			serializationProxy.write(outputView);
		}

		@Override
		protected void cleanupProvidedResources() {

		}
	}
}
