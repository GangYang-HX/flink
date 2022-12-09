package org.apache.flink.taishan.state.restore;

import com.bilibili.taishan.ClientManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.RestoreCheckpointException;
import org.apache.flink.runtime.checkpoint.RestoreFailureReason;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.taishan.state.TaishanSharedResources;
import org.apache.flink.taishan.state.bloomfilter.BloomFilterClient;
import org.apache.flink.taishan.state.bloomfilter.BloomFilterStore;
import org.apache.flink.taishan.state.bloomfilter.OffHeapBloomFilter;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.client.TaishanAdminConfiguration;
import org.apache.flink.taishan.state.client.TaishanClientConfiguration;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * @author Dove
 * @Date 2022/6/20 4:57 下午
 */
public class TaishanNormalRestoreOperation<K> extends AbstractTaishanRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanNormalRestoreOperation.class);

	private final KeyGroupRange keyGroupRange;
	private final CloseableRegistry cancelStreamRegistry;
	private final MetricGroup metricGroup;
	private final TaishanClientConfiguration taishanClientConfiguration;
	private final BloomFilterStore bloomFilterStore;
	private final int keyGroupPrefixBytes;
	private final CacheConfiguration cacheConfiguration;
	private final OpaqueMemoryResource<TaishanSharedResources> sharedResources;
	private final Collection<KeyedStateHandle> restoreStateHandles;
	/**
	 * Current input stream we obtained from currentKeyGroupsStateHandle.
	 */
	private FSDataInputStream currentStateHandleInStream;
	/**
	 * The compression decorator that was used for writing the state, as determined by the meta data.
	 */
	private StreamCompressionDecorator keygroupStreamCompressionDecorator;

	private Counter readMetadataTime;
	private String taishanTableName;
	private String accessToken;
	private TaishanAdminConfiguration taishanAdminConfiguration;

	// ------------------------------- rescale --------------------------------------------
	public static final String RESCALE_GROUP = "rescale";
	// ------------------------------- no-rescale --------------------------------------------
	public static final String NO_RESCALE_GROUP = "norescale";
	// ------------------------------- common --------------------------------------------
	public static final String READ_METADATA_TIME = "readMetadataTime";
	private TaishanKeyedStateHandle currentKeyGroupsStateHandle;
	private StreamStateHandle currentMetaStateHandle;

	public TaishanNormalRestoreOperation(ClientManager clientManager,
										 KeyGroupRange keyGroupRange,
										 CloseableRegistry cancelStreamRegistry,
										 ClassLoader userCodeClassLoader,
										 StateSerializerProvider<K> keySerializerProvider,
										 Collection<KeyedStateHandle> restoreStateHandles,
										 MetricGroup metricGroup,
										 LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation,
										 TaishanClientConfiguration taishanClientConfiguration,
										 BloomFilterStore bloomFilterStore,
										 int keyGroupPrefixBytes,
										 CacheConfiguration cacheConfiguration,
										 OpaqueMemoryResource<TaishanSharedResources> sharedResources) {
		super(clientManager, userCodeClassLoader, keySerializerProvider, kvStateInformation);
		this.restoreStateHandles = restoreStateHandles;
		this.keyGroupRange = keyGroupRange;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.metricGroup = metricGroup;
		this.taishanClientConfiguration = taishanClientConfiguration;
		this.bloomFilterStore = bloomFilterStore;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.cacheConfiguration = cacheConfiguration;
		this.sharedResources = sharedResources;
	}

	@Override
	public TaishanRestoreResult restore() throws Exception {
		long start = System.currentTimeMillis();
		final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();
		restoreTaishanCheckpoint(theFirstStateHandle);

		boolean isRescaling = (restoreStateHandles.size() > 1 ||
			!Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

		registRtHistograms(isRescaling);

		for (KeyedStateHandle restoreStateHandle : restoreStateHandles) {
			try {
				checkKeyedStateHandle(restoreStateHandle);
				currentKeyGroupsStateHandle = (TaishanKeyedStateHandle) theFirstStateHandle;
				currentMetaStateHandle = currentKeyGroupsStateHandle.getMetaStateHandle();
				currentStateHandleInStream = currentMetaStateHandle.openInputStream();
				cancelStreamRegistry.registerCloseable(currentStateHandleInStream);
				restoreKVStateMetaData();
				restoreBloomFilter();
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(currentStateHandleInStream)) {
					IOUtils.closeQuietly(currentStateHandleInStream);
				}
			}
		}

		LOG.info("Taishan stateBackend restore with {} cost {}ms.", isRescaling ? "rescale" : "noRescale",
			System.currentTimeMillis() - start);
		return new TaishanRestoreResult(getTaishanClientWrapper(), taishanTableName, accessToken);
	}

	/**
	 * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current state handle.
	 */
	private void restoreKVStateMetaData() throws IOException, StateMigrationException {
		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(currentMetaStateHandle);
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
		registerColumnFamilyHandles(stateMetaInfoSnapshots);

		this.keygroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
			SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;
	}

	private void checkKeyedStateHandle(KeyedStateHandle keyedStateHandle) {
		if (!(keyedStateHandle instanceof TaishanKeyedStateHandle)) {
			IllegalStateException e = new IllegalStateException("Unexpected state handle type, " +
				"expected: " + TaishanKeyedStateHandle.class +
				", but found: " + keyedStateHandle.getClass());
			throw new RestoreCheckpointException(RestoreFailureReason.UNEXPECTED_STATE_HANDLE_TYPE, e);
		}
	}

	private void restoreTaishanCheckpoint(KeyedStateHandle theFirstStateHandle) throws IOException {
		checkKeyedStateHandle(theFirstStateHandle);
		TaishanKeyedStateHandle taishanKeyedStateHandle = (TaishanKeyedStateHandle) theFirstStateHandle;
		taishanTableName = taishanKeyedStateHandle.getTaishanTableName();
		accessToken = taishanKeyedStateHandle.getAccessToken();
		String taishanSnapshotId = taishanKeyedStateHandle.getTaishanSnapshotId();
		taishanAdminConfiguration = TaishanAdminConfiguration.restoreAdminConfiguration(taishanKeyedStateHandle);
		openClient(taishanTableName, accessToken, taishanClientConfiguration);
		for (int keyGroup = keyGroupRange.getStartKeyGroup(); keyGroup <= keyGroupRange.getEndKeyGroup(); keyGroup++) {
			LOG.debug("switch checkpoint keyGroup:{}, taishanSnapshotId:{}", keyGroup, taishanSnapshotId);
			taishanClientWrapper.switchCheckpoint(keyGroup, taishanSnapshotId);
		}
		LOG.info("switch checkpoint keyGroupRange:{}, taishanSnapshotId:{}, taishanTable:{}", keyGroupRange, taishanSnapshotId, taishanTableName);
	}

	private void restoreBloomFilter() {
		// restore bloomFilter from taishan or Empty
		for (String stateName : kvStateInformation.keySet()) {
			BloomFilterClient filterClient = bloomFilterStore.getOrInitFilterClient(stateName, keyGroupRange, keyGroupPrefixBytes, taishanClientWrapper);
			if (cacheConfiguration.isAllKeyBF()) {
				OffHeapBloomFilter offHeapBloomFilter = (OffHeapBloomFilter) filterClient;
				offHeapBloomFilter.isRestore();
			}
		}
	}

	@Override
	public TaishanAdminConfiguration getTaishanAdminConfiguration() {
		return taishanAdminConfiguration;
	}

	private void registRtHistograms(boolean isRescaling) {
		MetricGroup metricGroup;
		if (isRescaling) {
			metricGroup = this.metricGroup.addGroup(RESCALE_GROUP);
		} else {
			metricGroup = this.metricGroup.addGroup(NO_RESCALE_GROUP);
		}
		readMetadataTime = metricGroup.counter(READ_METADATA_TIME, new SimpleCounter());
	}

	private void registerColumnFamilyHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots) {
		// Currently, Only record the metrics of the whole stage.
		for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
			getOrRegisterStateColumnFamilyHandle(metaInfoSnapshots.get(i));
		}
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle) throws IOException, StateMigrationException {
		InputStream inputStream = null;
		long readMetadataStart = System.nanoTime();

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
