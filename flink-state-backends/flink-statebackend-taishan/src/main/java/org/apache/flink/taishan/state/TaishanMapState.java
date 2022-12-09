package org.apache.flink.taishan.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.taishan.state.cache.*;
import org.apache.flink.taishan.state.client.TaishanClientBatchDelWrapper;
import org.apache.flink.taishan.state.client.TaishanClientBatchWriteWrapper;
import org.apache.flink.taishan.state.client.TaishanClientIteratorWrapper;
import org.apache.flink.taishan.state.serialize.TaishanSerializedCompositeKeyBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.apache.flink.taishan.state.cache.OffHeapUtils.cacheKeyWrapper;

/**
 * @author Dove
 * @Date 2022/6/20 1:59 下午
 */
public class TaishanMapState<K, N, UK, UV>
	extends AbstractTaishanState<K, N, Map<UK, UV>>
	implements InternalMapState<K, N, UK, UV> {

	private static final Logger LOG = LoggerFactory.getLogger(TaishanMapState.class);

	public static final int CACHE_SIZE_LIMIT = 1024;

	/**
	 * Serializer for the keys and values.
	 */
	private final TypeSerializer<UK> userKeySerializer;
	private final TypeSerializer<UV> userValueSerializer;

	public TaishanMapState(String stateName,
						   TypeSerializer<N> namespaceSerializer,
						   TypeSerializer<Map<UK, UV>> valueSerializer,
						   TaishanKeyedStateBackend<K> backend,
						   Map<UK, UV> defaultValue,
						   StateTtlConfig ttlConfig,
						   TaishanMetric taishanMetric) {
		super(stateName, namespaceSerializer, valueSerializer, backend, defaultValue, ttlConfig, taishanMetric);

		MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
		this.userKeySerializer = castedMapSerializer.getKeySerializer();
		this.userValueSerializer = castedMapSerializer.getValueSerializer();
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

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

		final byte[] keyPrefixBytes = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);

		final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

		final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
		final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();
		final DataInputDeserializer inputView = new DataInputDeserializer();

		final Iterator<Map.Entry<UK, UV>> iterator = new TaishanMapIterator<Map.Entry<UK, UV>>(
			cacheClient,
			keyPrefixBytes,
			dupUserKeySerializer,
			dupUserValueSerializer,
			inputView,
			keyGroupId) {
			@Override
			public Map.Entry<UK, UV> next() {
				return nextEntry();
			}
		};

		// Return null to make the behavior consistent with other backends
		if (!iterator.hasNext()) {
			return null;
		}

		return KvStateSerializer.serializeMap(() -> iterator, dupUserKeySerializer, dupUserValueSerializer);
	}

	@Override
	public UV get(UK key) throws Exception {
		byte[] rangeKey = serializeRangeKeyWithNamespaceAndUserKey(key, userKeySerializer);
		if (!bloomFilter.mightContains(keyGroupId(), rangeKey)) {
			return null;
		}

		byte[] valueBytes = cacheClient.get(keyGroupId(), rangeKey);
		byte[] finalValueBytes = decompress(valueBytes);
		return finalValueBytes == null ? null : deserializeUserValue(dataInputView, finalValueBytes, userValueSerializer);
	}

	@Override
	public void put(UK key, UV value) throws Exception {
		byte[] rangeKey = serializeRangeKeyWithNamespaceAndUserKey(key, userKeySerializer);
		byte[] valueByte = serializeValueNullSensitive(value, userValueSerializer);
		byte[] compressValueByte = compress(valueByte);
		int ttlTime = getTtlTime();
		cacheClient.update(keyGroupId(), rangeKey, compressValueByte, ttlTime);
		bloomFilter.add(keyGroupId(), rangeKey, ttlTime);
	}

	@Override
	public void putAll(Map<UK, UV> map) throws Exception {
		if (map == null) {
			return;
		}
		int ttlTime = getTtlTime();
		if (cacheEnabled) {
			for (Map.Entry<UK, UV> entry : map.entrySet()) {
				byte[] rangeKey = serializeRangeKeyWithNamespaceAndUserKey(entry.getKey(), userKeySerializer);
				byte[] valueBytes = serializeValueNullSensitive(entry.getValue(), userValueSerializer);
				byte[] compressValueBytes = compress(valueBytes);
				cacheClient.update(keyGroupId(), rangeKey, compressValueBytes, ttlTime);
				bloomFilter.add(keyGroupId(), rangeKey, ttlTime);
			}
		} else {
			try (TaishanClientBatchWriteWrapper writeBatchWrapper = new TaishanClientBatchWriteWrapper(cacheClient.getTaishanClientWrapper(), taishanMetric)) {
				taishanMetric.updateCatchingRate();
				long currentTimeMillis = System.currentTimeMillis();
				for (Map.Entry<UK, UV> entry : map.entrySet()) {
					byte[] rangeKey = serializeRangeKeyWithNamespaceAndUserKey(entry.getKey(), userKeySerializer);
					byte[] valueBytes = serializeValueNullSensitive(entry.getValue(), userValueSerializer);
					byte[] compressValueBytes = compress(valueBytes);
					CacheEntry cacheEntry = new CacheEntry(keyGroupId(), compressValueBytes, currentTimeMillis, CacheEntry.WRITE_ACTION, ttlTime);
					writeBatchWrapper.put(keyGroupId(), new ByteArrayWrapper(rangeKey), cacheEntry);
					bloomFilter.add(keyGroupId(), rangeKey, ttlTime);
				}
			}
		}
	}

	@Override
	public void remove(UK key) throws Exception {
		byte[] rangeKey = serializeRangeKeyWithNamespaceAndUserKey(key, userKeySerializer);
		cacheClient.delete(keyGroupId(), rangeKey);
		bloomFilter.delete(keyGroupId(), rangeKey);
	}

	@Override
	public boolean contains(UK key) throws Exception {
		byte[] rangeKey = serializeRangeKeyWithNamespaceAndUserKey(key, userKeySerializer);
		if (!bloomFilter.mightContains(keyGroupId(), rangeKey)) {
			return false;
		}

		byte[] rawValueBytes = cacheClient.get(keyGroupId(), rangeKey);
		return (rawValueBytes != null);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		// todo miss test case
		return this::iterator;
	}

	@Override
	public Iterable<UK> keys() throws Exception {
		final byte[] prefixBytes = serializeRangeKeyWithNamespace();

		return () -> new TaishanMapIterator<UK>(cacheClient, prefixBytes, userKeySerializer, userValueSerializer, dataInputView, keyGroupId()) {
			@Nullable
			@Override
			public UK next() {
				TaishanMapEntry entry = nextEntry();
				return (entry == null ? null : entry.getKey());
			}
		};
	}

	@Override
	public Iterable<UV> values() throws Exception {
		final byte[] prefixBytes = serializeRangeKeyWithNamespace();

		return () -> new TaishanMapIterator<UV>(cacheClient, prefixBytes, userKeySerializer, userValueSerializer, dataInputView, keyGroupId()) {
			@Override
			public UV next() {
				TaishanMapEntry entry = nextEntry();
				return (entry == null ? null : entry.getValue());
			}
		};
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		final byte[] prefixBytes = serializeRangeKeyWithNamespace();

		return new TaishanMapIterator<Map.Entry<UK, UV>>(cacheClient, prefixBytes, userKeySerializer, userValueSerializer, dataInputView, keyGroupId()) {
			@Override
			public Map.Entry<UK, UV> next() {
				return nextEntry();
			}
		};
	}

	@Override
	public boolean isEmpty() throws Exception {
		final byte[] prefixBytes = serializeRangeKeyWithNamespace();
		if (!cacheClient.isEmpty()) {
			return false;
		}
		cacheClient.flushBatchWrites();
		// Limit must be set to 2, and the Taishan Scan will remove the last piece of data
		TaishanClientIteratorWrapper scan = new TaishanClientIteratorWrapper(cacheClient.getTaishanClientWrapper(), keyGroupId(), prefixBytes, prefixBytes, 2, taishanMetric, true);
		if (scan.hasNext()) {
			scan.next();
			return !startWithKeyPrefix(prefixBytes, scan.key());
		}
		return true;
	}

	@Override
	public void clear() {
		final byte[] prefixBytes = serializeRangeKeyWithNamespace();
		cacheClient.flushBatchWrites();
		Set<ByteArrayWrapper> removeSet = new HashSet<>();
		try {
			TaishanClientIteratorWrapper scan = new TaishanClientIteratorWrapper(cacheClient.getTaishanClientWrapper(), keyGroupId(), prefixBytes, prefixBytes, CACHE_SIZE_LIMIT, taishanMetric, true);
			try (TaishanClientBatchDelWrapper taishanClientBatchDelWrapper = new TaishanClientBatchDelWrapper(cacheClient.getTaishanClientWrapper(), taishanMetric)) {
				while (scan.hasNext()) {
					scan.next();
					byte[] keyBytes = scan.key();
					if (startWithKeyPrefix(prefixBytes, keyBytes)) {
						taishanClientBatchDelWrapper.remove(keyGroupId(), new ByteArrayWrapper(keyBytes));
						removeSet.add(new ByteArrayWrapper(cacheKeyWrapper(keyBytes, offHeapKeyPre, isOffHeap)));
						bloomFilter.delete(keyGroupId(), keyBytes);
					} else {
						break;
					}
				}
			}
			cacheClient.removeAll(keyGroupId(), removeSet);
		} catch (Exception e) {
			LOG.warn("Error while cleaning the state.", e);
		}
	}

	private static <UK> UK deserializeUserKey(
		DataInputDeserializer dataInputView,
		int userKeyOffset,
		byte[] rawKeyBytes,
		TypeSerializer<UK> keySerializer) throws IOException {
		dataInputView.setBuffer(rawKeyBytes, userKeyOffset, rawKeyBytes.length - userKeyOffset);
		return keySerializer.deserialize(dataInputView);
	}

	private static <UV> UV deserializeUserValue(
		DataInputDeserializer dataInputView,
		byte[] rawValueBytes,
		TypeSerializer<UV> valueSerializer) throws IOException {

		dataInputView.setBuffer(rawValueBytes);

		boolean isNull = dataInputView.readBoolean();

		return isNull ? null : valueSerializer.deserialize(dataInputView);
	}

	private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
		if (rawKeyBytes.length < keyPrefixBytes.length) {
			return false;
		}
		// keyPrefixBytes = #ColumnFamily#Key#Namespace
		// rawKeyBytes =    #ColumnFamily#Key#Namespace[#UserKey]
		// check equal #Key#Namespace.(without #ColumnFamily)
		for (int i = keyPrefixBytes.length; --i >= sharedRangeKeyBuilder.getColumnFamilyLength(); ) {
			if (rawKeyBytes[i] != keyPrefixBytes[i]) {
				return false;
			}
		}
		return true;
	}

	// ------------------------------------------------------------------------
	//  Internal Classes
	// ------------------------------------------------------------------------

	/**
	 * A map entry in TaishanMapState.
	 */
	private class TaishanMapEntry implements Map.Entry<UK, UV> {
		/**
		 * The raw bytes of the key stored in Taishan. Each user key is stored in Taishan
		 * with the format #KeyGroup#Key#Namespace#UserKey.
		 */
		private final byte[] rawKeyBytes;

		/**
		 * The raw bytes of the value stored in Taishan.
		 */
		private byte[] rawValueBytes;

		/**
		 * True if the entry has been deleted.
		 */
		private boolean deleted;

		/**
		 * The user key and value. The deserialization is performed lazily, i.e. the key
		 * and the value is deserialized only when they are accessed.
		 */
		private UK userKey;

		private UV userValue;

		/**
		 * The offset of User Key offset in raw key bytes.
		 */
		private final int userKeyOffset;

		private final TypeSerializer<UK> keySerializer;

		private final TypeSerializer<UV> valueSerializer;

		private final DataInputDeserializer dataInputView;

		TaishanMapEntry(
			@Nonnegative final int userKeyOffset,
			@Nonnull final byte[] rawKeyBytes,
			@Nonnull final byte[] rawValueBytes,
			@Nonnull final TypeSerializer<UK> keySerializer,
			@Nonnull final TypeSerializer<UV> valueSerializer,
			@Nonnull DataInputDeserializer dataInputView) {
			this.userKeyOffset = userKeyOffset;
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;

			this.rawKeyBytes = rawKeyBytes;
			this.rawValueBytes = rawValueBytes;
			this.deleted = false;
			this.dataInputView = dataInputView;
		}

		public void remove() {
			deleted = true;
			rawValueBytes = null;
			cacheClient.delete(keyGroupId(), rawKeyBytes);
			bloomFilter.delete(keyGroupId(), rawKeyBytes);
		}

		@Override
		public UK getKey() {
			if (userKey == null) {
				try {
					userKey = deserializeUserKey(dataInputView, userKeyOffset, rawKeyBytes, keySerializer);
				} catch (IOException e) {
					throw new FlinkRuntimeException("Error while deserializing the user key.", e);
				}
			}

			return userKey;
		}

		@Override
		public UV getValue() {
			if (deleted) {
				return null;
			} else {
				if (userValue == null) {
					try {
						userValue = deserializeUserValue(dataInputView, rawValueBytes, valueSerializer);
					} catch (IOException e) {
						throw new FlinkRuntimeException("Error while deserializing the user value.", e);
					}
				}

				return userValue;
			}
		}

		@Override
		public UV setValue(UV value) {
			if (deleted) {
				throw new IllegalStateException("The value has already been deleted.");
			}

			UV oldValue = getValue();

			try {
				userValue = value;
				rawValueBytes = serializeValueNullSensitive(value, valueSerializer);
				byte[] compressedValue = compress(rawValueBytes);
				int ttlTime = getTtlTime();
				cacheClient.update(keyGroupId(), rawKeyBytes, compressedValue, ttlTime);
				bloomFilter.add(keyGroupId(), rawKeyBytes, ttlTime);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Error while putting data into Taishan.", e);
			}

			return oldValue;
		}
	}

	/**
	 * An auxiliary utility to scan all entries under the given key.
	 */
	private abstract class TaishanMapIterator<T> implements Iterator<T> {

		/**
		 * The prefix bytes of the key being accessed. All entries under the same key
		 * have the same prefix, hence we can stop iterating once coming across an
		 * entry with a different prefix.
		 * #ColumnFamily#Key#Namespace.
		 */
		@Nonnull
		private final byte[] keyPrefixBytes;

		/**
		 * True if all entries have been accessed or the iterator has come across an
		 * entry with a different prefix.
		 */
		private boolean expired = false;

		/**
		 * A in-memory cache for the entries in the Taishan.
		 */
		private ArrayList<TaishanMapEntry> cacheEntries = new ArrayList<>();

		/**
		 * The entry pointing to the current position which is last returned by calling {@link #nextEntry()}.
		 */
		private TaishanMapEntry currentEntry;
		private int cacheIndex = 0;

		private final TypeSerializer<UK> keySerializer;
		private final TypeSerializer<UV> valueSerializer;
		private final DataInputDeserializer dataInputView;
		private final int keyGroupId;

		private final SnapshotableCacheClient cacheClient;

		TaishanMapIterator(
			final SnapshotableCacheClient cacheClient,
			final byte[] keyPrefixBytes,
			final TypeSerializer<UK> keySerializer,
			final TypeSerializer<UV> valueSerializer,
			DataInputDeserializer dataInputView,
			int keyGroupId) {

			this.keyPrefixBytes = keyPrefixBytes;
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
			this.dataInputView = dataInputView;
			this.keyGroupId = keyGroupId;
			this.cacheClient = cacheClient;
		}

		@Override
		public boolean hasNext() {
			loadCache();
			return (cacheIndex < cacheEntries.size());
		}

		@Override
		public void remove() {
			if (currentEntry == null || currentEntry.deleted) {
				throw new IllegalStateException("The remove operation must be called after a valid next operation.");
			}

			currentEntry.remove();
		}

		final TaishanMapEntry nextEntry() {
			loadCache();

			if (cacheIndex == cacheEntries.size()) {
				if (!expired) {
					throw new IllegalStateException();
				}

				return null;
			}

			this.currentEntry = cacheEntries.get(cacheIndex);
			cacheIndex++;

			return currentEntry;
		}

		private void loadCache() {
			if (cacheIndex > cacheEntries.size()) {
				throw new IllegalStateException();
			}

			// Load cache entries only when the cache is empty and there still exist unread entries
			if (cacheIndex < cacheEntries.size() || expired) {
				return;
			}
			cacheClient.flushBatchWrites(keyGroupId);
			/*
			 * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
			 * the currentEntry points to the last returned entry, and at that time, we will start
			 * the iterating from currentEntry if reloading cache is needed.
			 */
			byte[] startBytes = (currentEntry == null ? keyPrefixBytes : currentEntry.rawKeyBytes);

			cacheEntries.clear();
			cacheIndex = 0;

			TaishanClientIteratorWrapper iterator = new TaishanClientIteratorWrapper(cacheClient.getTaishanClientWrapper(), keyGroupId, keyPrefixBytes, startBytes, CACHE_SIZE_LIMIT, taishanMetric, true);

			/*
			 * If the entry pointing to the current position is not removed, it will be the first entry in the
			 * new iterating. Skip it to avoid redundant access in such cases.
			 */
			if (currentEntry != null && !currentEntry.deleted) {
				// todo miss takeNextMetrics
				iterator.next();
			}

			while (iterator.hasNext()) {
				// todo miss takeNextMetrics
				iterator.next();

				if (!startWithKeyPrefix(keyPrefixBytes, iterator.key())) {
					expired = true;
					break;
				}

				if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
					break;
				}
				byte[] valRawBytes = iterator.value();
				byte[] decompressValRawBytes = decompress(valRawBytes);
				TaishanMapEntry entry = new TaishanMapEntry(
					keyPrefixBytes.length,
					iterator.key(),
					decompressValRawBytes,
					keySerializer,
					valueSerializer,
					dataInputView);

				cacheEntries.add(entry);
			}
		}
	}

	@SuppressWarnings("unchecked")
	static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
		TaishanKeyedStateBackend<K> backend,
		TaishanMetric taishanMetric) {
		return (IS) new TaishanMapState<K, N, UK, UV>(
			stateDesc.getName(),
			keyValueStateBackendMetaInfo.getNamespaceSerializer(),
			(TypeSerializer<Map<UK, UV>>) keyValueStateBackendMetaInfo.getStateSerializer(),
			backend,
			(Map<UK, UV>) stateDesc.getDefaultValue(),
			stateDesc.getTtlConfig(),
			taishanMetric);
	}

	/**
	 * Taishan map state specific byte value transformer wrapper.
	 *
	 * <p>This specific transformer wrapper checks the first byte to detect null user value entries
	 * and if not null forward the rest of byte array to the original byte value transformer.
	 */
	static class StateSnapshotTransformerWrapper implements StateSnapshotTransformer<byte[]> {
		private static final byte[] NULL_VALUE;
		private static final byte NON_NULL_VALUE_PREFIX;

		static {
			DataOutputSerializer dov = new DataOutputSerializer(1);
			try {
				dov.writeBoolean(true);
				NULL_VALUE = dov.getCopyOfBuffer();
				dov.clear();
				dov.writeBoolean(false);
				NON_NULL_VALUE_PREFIX = dov.getSharedBuffer()[0];
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to serialize boolean flag of map user null value", e);
			}
		}

		private final StateSnapshotTransformer<byte[]> elementTransformer;
		private final DataInputDeserializer div;

		StateSnapshotTransformerWrapper(StateSnapshotTransformer<byte[]> originalTransformer) {
			this.elementTransformer = originalTransformer;
			this.div = new DataInputDeserializer();
		}

		@Override
		@Nullable
		public byte[] filterOrTransform(@Nullable byte[] value) {
			if (value == null || isNull(value)) {
				return NULL_VALUE;
			} else {
				// we have to skip the first byte indicating null user value
				// TODO: optimization here could be to work with slices and not byte arrays
				// and copy slice sub-array only when needed
				byte[] woNullByte = Arrays.copyOfRange(value, 1, value.length);
				byte[] filteredValue = elementTransformer.filterOrTransform(woNullByte);
				if (filteredValue == null) {
					filteredValue = NULL_VALUE;
				} else if (filteredValue != woNullByte) {
					filteredValue = prependWithNonNullByte(filteredValue, value);
				} else {
					filteredValue = value;
				}
				return filteredValue;
			}
		}

		private boolean isNull(byte[] value) {
			try {
				div.setBuffer(value, 0, 1);
				return div.readBoolean();
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to deserialize boolean flag of map user null value", e);
			}
		}

		private static byte[] prependWithNonNullByte(byte[] value, byte[] reuse) {
			int len = 1 + value.length;
			byte[] result = reuse.length == len ? reuse : new byte[len];
			result[0] = NON_NULL_VALUE_PREFIX;
			System.arraycopy(value, 0, result, 1, value.length);
			return result;
		}
	}
}
