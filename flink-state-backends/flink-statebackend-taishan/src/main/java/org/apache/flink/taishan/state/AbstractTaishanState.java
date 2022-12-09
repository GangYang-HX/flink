package org.apache.flink.taishan.state;

import com.github.luben.zstd.Zstd;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.taishan.state.bloomfilter.BloomFilterClient;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.taishan.state.cache.CacheEntry;
import org.apache.flink.taishan.state.cache.CacheBufferClient;
import org.apache.flink.taishan.state.cache.NoneCacheClient;
import org.apache.flink.taishan.state.cache.SnapshotableCacheClient;
import org.apache.flink.taishan.state.cache.TCache;
import org.apache.flink.taishan.state.cache.TCacheHeap;
import org.apache.flink.taishan.state.cache.TCacheOffHeap;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.taishan.state.serialize.TaishanSerializedCompositeKeyBuilder;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractTaishanState<K, N, V> implements InternalKvState<K, N, V>, State {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractTaishanState.class);
	protected final TypeSerializer<V> valueSerializer;
	protected final TaishanKeyedStateBackend<K> backend;
	protected final TypeSerializer<N> namespaceSerializer;

	protected final V defaultValue;

	protected final DataOutputSerializer dataOutputView;
	protected final DataInputDeserializer dataInputView;
	protected final TaishanSerializedCompositeKeyBuilder<K> sharedRangeKeyBuilder;
	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace;
	protected final String stateName;
	private final StateTtlConfig stateTtlConfig;

	protected TaishanMetric taishanMetric;

	protected boolean cacheEnabled;

	protected SnapshotableCacheClient cacheClient;

	public BloomFilterClient bloomFilter;

	protected boolean compressEnabled;
	protected byte[] offHeapKeyPre;
	protected boolean isOffHeap;
	private final Boolean isTtlState;

	protected AbstractTaishanState(
		String stateName,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<V> valueSerializer,
		TaishanKeyedStateBackend<K> backend,
		V defaultValue,
		StateTtlConfig stateTtlConfig,
		TaishanMetric taishanMetric) {
		this.stateName = stateName;
		this.valueSerializer = valueSerializer;
		this.backend = backend;
		this.namespaceSerializer = namespaceSerializer;
		this.defaultValue = defaultValue;
		this.stateTtlConfig = stateTtlConfig;
		this.dataOutputView = new DataOutputSerializer(128);
		this.dataInputView = new DataInputDeserializer();
		this.sharedRangeKeyBuilder = backend.getSharedRangeKeyBuilder(stateName);
		this.taishanMetric = taishanMetric;

		initializeCacheClient(stateName, backend.getCacheConfiguration(), backend.taishanClientWrapper);

		// init bloomFilter
		this.bloomFilter = Preconditions.checkNotNull(this.backend.getBloomFilterStore().getFilterClient(stateName), "BloomFilter must not be null.");

		if (valueSerializer instanceof MapSerializer) {
			isTtlState = ((MapSerializer<?, ?>) valueSerializer).getValueSerializer() instanceof TtlStateFactory.TtlSerializer;
		} else {
			isTtlState = valueSerializer instanceof TtlStateFactory.TtlSerializer;
		}

		ExecutionConfig config = backend.getExecutionConfig();
		this.compressEnabled = config.rocksDBCompressEnabled;
	}

	protected void initializeCacheClient(String stateName, CacheConfiguration cacheConfiguration, TaishanClientWrapper taishanClientWrapper) {
		TCache.HeapType heapType = cacheConfiguration.getHeapType();
		this.isOffHeap = cacheConfiguration.isOffHeap();
		this.offHeapKeyPre = taishanClientWrapper.getOffHeapKeyPre();
		switch (heapType) {
			case HEAP:
				KeyGroupRange keyGroupRange = backend.getKeyGroupRange();
				int startKeyGroup = keyGroupRange.getStartKeyGroup();
				int endKeyGroup = keyGroupRange.getEndKeyGroup();
				TCacheHeap cache = new TCacheHeap(startKeyGroup, endKeyGroup, cacheConfiguration);
				this.cacheClient = new CacheBufferClient(taishanClientWrapper, this.backend.getCacheConfiguration(), this.backend.getKeyGroupRange(), cache, offHeapKeyPre, taishanMetric);
				this.cacheEnabled = true;
				break;
			case OFFHEAP:
				TCacheOffHeap<CacheEntry> offHeap = this.backend.getTaishanSharedResources().getCacheKV();
				offHeap.setDataTtl(stateTtlConfig.getTtl().getSize());
				this.cacheClient = new CacheBufferClient(taishanClientWrapper, this.backend.getCacheConfiguration(), this.backend.getKeyGroupRange(), offHeap, offHeapKeyPre, taishanMetric);
				this.cacheEnabled = true;
				break;
			case NONE:
				this.cacheClient = new NoneCacheClient(taishanMetric, taishanClientWrapper);
				this.cacheEnabled = false;
				break;
			default:
				throw new FlinkRuntimeException("Only three heap types are supported(heap/offheap/none).");
		}
		this.backend.getCacheGroup().addCacheTaishanClient(stateName, cacheClient);
	}


	@Override
	public void clear() {
		byte[] rangeKey = serializeRangeKeyWithNamespace();
		cacheClient.delete(keyGroupId(), rangeKey);
		bloomFilter.delete(keyGroupId(), rangeKey);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return backend.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<V> safeValueSerializer) throws Exception {
		// todo miss test case
		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		TaishanSerializedCompositeKeyBuilder<K> keyBuilder =
			new TaishanSerializedCompositeKeyBuilder<K>(
				stateName,
				safeKeySerializer,
				backend.getKeyGroupPrefixBytes(),
				32);
		int keyGroupId = KeyGroupRangeAssignment.assignToKeyGroup(keyAndNamespace.f0, backend.getNumberOfKeyGroups());
		keyBuilder.setKey(keyAndNamespace.f0, keyGroupId);
		byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
		byte[] value = cacheClient.get(keyGroupId, key);
		return decompress(value);
	}

	@Override
	public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		throw new UnsupportedOperationException("Global state entry iterator is unsupported for Taishan backend");
	}

	/**
	 * @return The unit is in seconds
	 */
	public int getTtlTime() {
		if (stateTtlConfig.isEnabled()) {
			long ttlSize = (System.currentTimeMillis() + stateTtlConfig.getTtl().getSize()) / 1000;
			return ttlSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSize;
		}
		return 0;
	}

	protected V getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}

	byte[] serializeRangeKeyWithNamespace() {
		return sharedRangeKeyBuilder.buildCompositeKeyNamespace(currentNamespace, namespaceSerializer);
	}

	<T> byte[] serializeRangeKeyWithNamespaceAndUserKey(T userKey, TypeSerializer<T> userKeySerializer) throws IOException {
		return sharedRangeKeyBuilder.buildCompositeKeyNamesSpaceUserKey(
			currentNamespace,
			namespaceSerializer,
			userKey,
			userKeySerializer
		);
	}

	int keyGroupId() {
		return sharedRangeKeyBuilder.getKeyGroupId();
	}

	<T> byte[] serializeValue(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		return serializeValueInternal(value, serializer);
	}

	<T> byte[] serializeValueNullSensitive(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		dataOutputView.writeBoolean(value == null);
		return serializeValueInternal(value, serializer);
	}

	<T> byte[] serializeValueList(
		List<T> valueList,
		TypeSerializer<T> elementSerializer,
		byte delimiter) throws IOException {

		dataOutputView.clear();
		boolean first = true;

		for (T value : valueList) {
			Preconditions.checkNotNull(value, "You cannot add null to a value list.");

			if (first) {
				first = false;
			} else {
				dataOutputView.write(delimiter);
			}
			elementSerializer.serialize(value, dataOutputView);
		}

		return dataOutputView.getCopyOfBuffer();
	}

	byte[] getKeyBytes() {
		return serializeRangeKeyWithNamespace();
	}

	byte[] getValueBytes(V value) {
		try {
			dataOutputView.clear();
			valueSerializer.serialize(value, dataOutputView);
			return dataOutputView.getCopyOfBuffer();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing value", e);
		}
	}

	private <T> byte[] serializeValueInternal(T value, TypeSerializer<T> serializer) throws IOException {
		serializer.serialize(value, dataOutputView);
		return dataOutputView.getCopyOfBuffer();
	}

	protected byte[] compress(byte[] val) {
		if (compressEnabled && val != null) {
			if (isTtlState) {
				byte[] timeStamp = Arrays.copyOfRange(val, 0, 8);
				val = com.github.luben.zstd.Zstd.compress(Arrays.copyOfRange(val, 8, val.length), 3);
				return ArrayUtils.concat(timeStamp, val);
			} else {
				return com.github.luben.zstd.Zstd.compress(val);
			}
		}

		return val;
	}

	protected byte[] decompress(byte[] val) {
		if (compressEnabled && val != null) {
			if (isTtlState) {
				byte[] rawVal = Arrays.copyOfRange(val, 8, val.length);
				int length = (int) Zstd.decompressedSize(rawVal);
				rawVal = Zstd.decompress(rawVal, length);
				return ArrayUtils.concat(Arrays.copyOfRange(val, 0, 8), rawVal);
			} else {
				int length = (int) Zstd.decompressedSize(val);
				return Zstd.decompress(val, length);
			}
		}
		return val;
	}
}
