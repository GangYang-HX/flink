/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.contrib.streaming.state.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateDownloader;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.RestoreCheckpointException;
import org.apache.flink.runtime.checkpoint.RestoreFailureReason;
import org.apache.flink.runtime.speculativeframe.SpeculativeTasksManager;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.CLIP_DB_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.INIT_ROCKS_DB_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.ITERATE_DB_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.LINK_OR_COPY_FILES_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.LOAD_DB_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.NO_RESCALE_GROUP;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.READ_METADATA_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.REGIST_CF_DESC_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.REGIST_CF_HANDLE_TIME;
import static org.apache.flink.contrib.streaming.state.restore.RestoreMetricsConstant.RESCALE_GROUP;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Encapsulates the process of restoring a RocksDB instance from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final String operatorIdentifier;
	private final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;
	private long lastCompletedCheckpointId;
	private UUID backendUID;
	private final long writeBatchSize;
	private final long downloadTimeout;

	//------------------ metrics fields --------------
	//common
	private Counter initRocksDBTime;
	private Counter readMetadataTime;
	private Counter registCFDescTime;
	private Counter registCFHandleTime;

	//rescale
	@Nullable private Counter clipDBTime;
	@Nullable private Counter loadDBTime;
	@Nullable private Counter iterateDBTime;

	//no rescale
	@Nullable private Counter linkOrCopyFileTime;

	private final SpeculativeTasksManager speculativeTasksManager;

	private final int numberSlots;

	private final boolean speculativeDownloadingEnabled;



	public RocksDBIncrementalRestoreOperation(
		String operatorIdentifier,
		KeyGroupRange keyGroupRange,
		int keyGroupPrefixBytes,
		int numberOfTransferringThreads,
		CloseableRegistry cancelStreamRegistry,
		ClassLoader userCodeClassLoader,
		Map<String, RocksDbKvStateInfo> kvStateInformation,
		StateSerializerProvider<K> keySerializerProvider,
		File instanceBasePath,
		File instanceRocksDBPath,
		DBOptions dbOptions,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		RocksDBNativeMetricOptions nativeMetricOptions,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		@Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
		@Nonnegative long writeBatchSize,
		long downloadTimeout,
		SpeculativeTasksManager speculativeTasksManager,
		int numberSlots,
		boolean speculativeDownloadingEnabled) {
		super(keyGroupRange,
			keyGroupPrefixBytes,
			numberOfTransferringThreads,
			cancelStreamRegistry,
			userCodeClassLoader,
			kvStateInformation,
			keySerializerProvider,
			instanceBasePath,
			instanceRocksDBPath,
			dbOptions,
			columnFamilyOptionsFactory,
			nativeMetricOptions,
			metricGroup,
			restoreStateHandles,
			ttlCompactFiltersManager);
		this.operatorIdentifier = operatorIdentifier;
		this.restoredSstFiles = new TreeMap<>();
		this.lastCompletedCheckpointId = -1L;
		this.backendUID = UUID.randomUUID();
		checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
		this.writeBatchSize = writeBatchSize;
		this.downloadTimeout = downloadTimeout;
		this.speculativeTasksManager = speculativeTasksManager;
		this.numberSlots = numberSlots;
		this.speculativeDownloadingEnabled = speculativeDownloadingEnabled;
	}

	/**
	 * Root method that branches for different implementations of {@link KeyedStateHandle}.
	 */
	@Override
	public RocksDBRestoreResult restore() throws Exception {

		if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
			return null;
		}

		final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

		boolean isRescaling = (restoreStateHandles.size() > 1 ||
			!Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

		long start = System.currentTimeMillis();
		registRtHistograms(isRescaling);
		if (isRescaling) {
			restoreWithRescaling(restoreStateHandles);
		} else {
			restoreWithoutRescaling(theFirstStateHandle);
		}
		LOG.info("{} restore with {} cost {}ms", operatorIdentifier, isRescaling ? "rescale" : "noRescale",
				System.currentTimeMillis() - start);
		return new RocksDBRestoreResult(this.db, defaultColumnFamilyHandle,
			nativeMetricMonitor, lastCompletedCheckpointId, backendUID, restoredSstFiles);
	}

	/**
	 * Regist all histograms.
	 * @param isRescaling isRescaling
	 */
	private void registRtHistograms(boolean isRescaling) {
		MetricGroup metricGroup;
		if (isRescaling) {
			metricGroup = this.metricGroup.addGroup(RESCALE_GROUP);
			clipDBTime = metricGroup.counter(CLIP_DB_TIME, new SimpleCounter());
			loadDBTime = metricGroup.counter(LOAD_DB_TIME, new SimpleCounter());
			iterateDBTime = metricGroup.counter(ITERATE_DB_TIME, new SimpleCounter());
		} else {
			metricGroup = this.metricGroup.addGroup(NO_RESCALE_GROUP);
		}
		initRocksDBTime = metricGroup.counter(INIT_ROCKS_DB_TIME, new SimpleCounter());
		readMetadataTime = metricGroup.counter(READ_METADATA_TIME, new SimpleCounter());
		registCFDescTime = metricGroup.counter(REGIST_CF_DESC_TIME, new SimpleCounter());
		registCFHandleTime = metricGroup.counter(REGIST_CF_HANDLE_TIME, new SimpleCounter());
		linkOrCopyFileTime = metricGroup.counter(LINK_OR_COPY_FILES_TIME, new SimpleCounter());

	}


	/**
	 * Recovery from a single remote incremental state without rescaling.
	 */
	private void restoreWithoutRescaling(KeyedStateHandle keyedStateHandle) throws Exception {
		if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle =
				(IncrementalRemoteKeyedStateHandle) keyedStateHandle;
			restorePreviousIncrementalFilesStatus(incrementalRemoteKeyedStateHandle);
			restoreFromRemoteState(incrementalRemoteKeyedStateHandle);
		} else if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			IncrementalLocalKeyedStateHandle incrementalLocalKeyedStateHandle =
				(IncrementalLocalKeyedStateHandle) keyedStateHandle;
			restorePreviousIncrementalFilesStatus(incrementalLocalKeyedStateHandle);
			restoreFromLocalState(incrementalLocalKeyedStateHandle);
		} else {
			BackendBuildingException backendBuildingException = new BackendBuildingException("Unexpected state handle type, " +
				"expected " + IncrementalRemoteKeyedStateHandle.class + " or " + IncrementalLocalKeyedStateHandle.class +
				", but found " + keyedStateHandle.getClass());
			throw new RestoreCheckpointException(RestoreFailureReason.UNEXPECTED_STATE_HANDLE_TYPE, backendBuildingException);
		}
	}

	private void restorePreviousIncrementalFilesStatus(IncrementalKeyedStateHandle localKeyedStateHandle) {
		backendUID = localKeyedStateHandle.getBackendIdentifier();
		restoredSstFiles.put(
			localKeyedStateHandle.getCheckpointId(),
			localKeyedStateHandle.getSharedStateHandleIDs());
		lastCompletedCheckpointId = localKeyedStateHandle.getCheckpointId();
	}

	private void restoreFromRemoteState(IncrementalRemoteKeyedStateHandle stateHandle) throws Exception {
		// used as restore source for IncrementalRemoteKeyedStateHandle
		final Path tmpRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
		try {
			restoreFromLocalState(
				transferRemoteStateToLocalDirectory(tmpRestoreInstancePath, stateHandle));
		} finally {
			cleanUpPathQuietly(tmpRestoreInstancePath);
		}
	}

	private void restoreFromLocalState(IncrementalLocalKeyedStateHandle localKeyedStateHandle) throws Exception {
		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(localKeyedStateHandle.getMetaDataState());
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
		columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true);
		columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);

		Path restoreSourcePath = localKeyedStateHandle.getDirectoryStateHandle().getDirectory();

		LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			operatorIdentifier, backendUID);

		if (!instanceRocksDBPath.mkdirs()) {
			String errMsg = "Could not create RocksDB data directory: " + instanceBasePath.getAbsolutePath();
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}

		restoreInstanceDirectoryFromPath(restoreSourcePath, dbPath);

		long openStart = System.nanoTime();
		openDB();
		initRocksDBTime.inc(System.nanoTime() - openStart);

		registerColumnFamilyHandles(stateMetaInfoSnapshots);
	}

	private IncrementalLocalKeyedStateHandle transferRemoteStateToLocalDirectory(
		Path temporaryRestoreInstancePath,
		IncrementalRemoteKeyedStateHandle restoreStateHandle) throws Exception {

		try (RocksDBStateDownloader rocksDBStateDownloader =
				     new RocksDBStateDownloader(
							 numberOfTransferringThreads,
							 numberSlots,
							 downloadTimeout,
							 metricGroup,
							 speculativeTasksManager,
							 speculativeDownloadingEnabled)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}

		// since we transferred all remote state to a local directory, we can use the same code as for
		// local recovery.
		return new IncrementalLocalKeyedStateHandle(
			restoreStateHandle.getBackendIdentifier(),
			restoreStateHandle.getCheckpointId(),
			new DirectoryStateHandle(temporaryRestoreInstancePath),
			restoreStateHandle.getKeyGroupRange(),
			restoreStateHandle.getMetaStateHandle(),
			restoreStateHandle.getSharedState().keySet());
	}

	private void cleanUpPathQuietly(@Nonnull Path path) {
		try {
			FileUtils.deleteDirectory(path.toFile());
		} catch (IOException ex) {
			LOG.warn("Failed to clean up path " + path, ex);
		}
	}

	private void registerColumnFamilyHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots) {
		// Register CF handlers

		// Currently, Only record the metrics of the whole stage.
		long registStart = System.nanoTime();
		for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
			getOrRegisterStateColumnFamilyHandle(columnFamilyHandles.get(i), metaInfoSnapshots.get(i));
		}
		registCFHandleTime.inc(System.nanoTime() - registStart);
	}

	/**
	 * Recovery from multi incremental states with rescaling. For rescaling, this method creates a temporary
	 * RocksDB instance for a key-groups shard. All contents from the temporary instance are copied into the
	 * real restore instance and then the temporary instance is discarded.
	 */
	private void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		// Prepare for restore with rescaling
		KeyedStateHandle initialHandle = RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
			restoreStateHandles, keyGroupRange);
		long initStart = System.nanoTime();
		// Init base DB instance
		if (initialHandle != null) {
			restoreStateHandles.remove(initialHandle);
			initDBWithRescaling(initialHandle);
			clipDBTime.inc(System.nanoTime() - initStart);
		} else {
			openDB();
			initRocksDBTime.inc(System.nanoTime() - initStart);
		}
		// Transfer remaining key-groups from temporary instance into base DB
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		// Currently, Only record the metrics of the whole stage.
		long loadStart = System.nanoTime();
		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalRemoteKeyedStateHandle)) {
				IllegalStateException e = new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalRemoteKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
				throw new RestoreCheckpointException(RestoreFailureReason.UNEXPECTED_STATE_HANDLE_TYPE, e);
			}
			Path temporaryRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
			try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
				(IncrementalRemoteKeyedStateHandle) rawStateHandle,
				temporaryRestoreInstancePath);
				 RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(this.db, writeBatchSize)) {

				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);
					long getOrRegistCFHandleStart = System.nanoTime();
					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(
						null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i))
						.columnFamilyHandle;
					registCFHandleTime.inc(System.nanoTime() - getOrRegistCFHandleStart);
					long iterateStart = System.nanoTime();
					try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle, tmpRestoreDBInfo.readOptions)) {

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
								writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
							} else {
								// Since the iterator will visit the record according to the sorted order,
								// we can just break here.
								break;
							}

							iterator.next();
						}
					} // releases native iterator resources
					iterateDBTime.inc(System.nanoTime() - iterateStart);
				}
			} finally {
				cleanUpPathQuietly(temporaryRestoreInstancePath);
			}
		}
		loadDBTime.inc(System.nanoTime() - loadStart);
	}

	private void initDBWithRescaling(KeyedStateHandle initialHandle) throws Exception {

		assert (initialHandle instanceof IncrementalRemoteKeyedStateHandle);

		// 1. Restore base DB from selected initial handle
		restoreFromRemoteState((IncrementalRemoteKeyedStateHandle) initialHandle);

		if (!needToClip) {
			LOG.info("FIFO no need to delete entrys");
			return;
		}

		// 2. Clip the base DB instance
		try {
			RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
				db,
				columnFamilyHandles,
				keyGroupRange,
				initialHandle.getKeyGroupRange(),
				keyGroupPrefixBytes,
				writeBatchSize);
		} catch (RocksDBException e) {
			String errMsg = "Failed to clip DB after initialization.";
			LOG.error(errMsg, e);
			throw new BackendBuildingException(errMsg, e);
		}
	}

	/**
	 * Entity to hold the temporary RocksDB instance created for restore.
	 */
	private static class RestoredDBInstance implements AutoCloseable {

		@Nonnull
		private final RocksDB db;

		@Nonnull
		private final ColumnFamilyHandle defaultColumnFamilyHandle;

		@Nonnull
		private final List<ColumnFamilyHandle> columnFamilyHandles;

		@Nonnull
		private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		private final ReadOptions readOptions;

		private RestoredDBInstance(
			@Nonnull RocksDB db,
			@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
			@Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
			this.db = db;
			this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
			this.columnFamilyHandles = columnFamilyHandles;
			this.columnFamilyDescriptors = columnFamilyDescriptors;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			this.readOptions = RocksDBOperationUtils.createTotalOrderSeekReadOptions();
			this.readOptions.setReadaheadSize(0);
		}

		@Override
		public void close() {
			List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(columnFamilyDescriptors.size() + 1);
			columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
			RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamilyHandle);
			IOUtils.closeQuietly(defaultColumnFamilyHandle);
			IOUtils.closeAllQuietly(columnFamilyHandles);
			IOUtils.closeQuietly(db);
			IOUtils.closeAllQuietly(columnFamilyOptions);
			IOUtils.closeQuietly(readOptions);
		}
	}

	private RestoredDBInstance restoreDBInstanceFromStateHandle(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path temporaryRestoreInstancePath) throws Exception {

		try (RocksDBStateDownloader rocksDBStateDownloader =
				new RocksDBStateDownloader(
						numberOfTransferringThreads,
						numberSlots,
						downloadTimeout,
						metricGroup,
						speculativeTasksManager,
						speculativeDownloadingEnabled)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}

		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(restoreStateHandle.getMetaStateHandle());
		// read meta data
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		long openStart = System.nanoTime();
		RocksDB restoreDb = RocksDBOperationUtils.openDB(
				temporaryRestoreInstancePath.toString(),
				columnFamilyDescriptors,
				columnFamilyHandles,
				RocksDBOperationUtils.createColumnFamilyOptions(columnFamilyOptionsFactory, "default"),
				dbOptions);
		initRocksDBTime.inc(System.nanoTime() - openStart);

		return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
	}

	/**
	 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
	 */
	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
		boolean registerTtlCompactFilter) {
		long registStart = System.nanoTime();
		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			RegisteredStateMetaInfoBase metaInfoBase =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
			ColumnFamilyDescriptor columnFamilyDescriptor = RocksDBOperationUtils.createColumnFamilyDescriptor(
				metaInfoBase, columnFamilyOptionsFactory, registerTtlCompactFilter ? ttlCompactFiltersManager : null);
			columnFamilyDescriptors.add(columnFamilyDescriptor);
		}
		registCFDescTime.inc(System.nanoTime() - registStart);
		return columnFamilyDescriptors;
	}

	/**
	 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
	 * a local state.
	 */
	private void restoreInstanceDirectoryFromPath(Path source, String instanceRocksDBPath) throws IOException {
		final Path instanceRocksDBDirectory = Paths.get(instanceRocksDBPath);
		final Path[] files = FileUtils.listDirectory(source);

		// Currently, Only record the metrics of the whole stage.
		long prepareFileStart = System.nanoTime();
		for (Path file : files) {
			final String fileName = file.getFileName().toString();
			final Path targetFile = instanceRocksDBDirectory.resolve(fileName);
			if (fileName.endsWith(SST_FILE_SUFFIX)) {
				// hardlink'ing the immutable sst-files.
				Files.createLink(targetFile, file);
			} else if (fileName.endsWith(RocksDBStateDownloader.TARGET_FILE_TEMP_SUFFIX)) {
				// These files may not have been deleted by speculative execution,
				// and we do not process them.
			} else {
				// true copy for all other files.
				Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
			}
		}
		linkOrCopyFileTime.inc(System.nanoTime() - prepareFileStart);
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle) throws Exception {
		long readMetadataStart = System.nanoTime();
		InputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			KeyedBackendSerializationProxy<K> proxy = readMetaData(in);
			readMetadataTime.inc(System.nanoTime() - readMetadataStart);
			return proxy;
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}
}
