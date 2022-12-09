package org.apache.flink.taishan.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.taishan.state.serialize.TaishanSerializedCompositeKeyBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/**
 * @author Dove
 * @Date 2022/7/26 7:00 下午
 */
public class TaishanListState<K, N, V> extends AbstractTaishanState<K, N, List<V>>
	implements InternalListState<K, N, V> {

	private static final Logger LOG = LoggerFactory.getLogger(TaishanListState.class);

	private final TypeSerializer<V> elementSerializer;
	/**
	 * Separator of StringAppendTestOperator in Taishan.
	 */
	private static final byte DELIMITER = ',';

	protected TaishanListState(String stateName, TypeSerializer<N> namespaceSerializer, TypeSerializer<List<V>> valueSerializer, TaishanKeyedStateBackend<K> backend, List<V> defaultValue, StateTtlConfig stateTtlConfig, TaishanMetric taishanMetric) {
		super(stateName, namespaceSerializer, valueSerializer, backend, defaultValue, stateTtlConfig, taishanMetric);
		ListSerializer<V> castedListSerializer = (ListSerializer<V>) valueSerializer;
		this.elementSerializer = castedListSerializer.getElementSerializer();
	}

	@Override
	public Iterable<V> get() throws Exception {
		return getInternal();
	}

	@Override
	public void add(V value) throws Exception {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
		byte[] rangeKey = serializeRangeKeyWithNamespace();
		byte[] valueBytes = serializeValue(value, elementSerializer);
		mergeValue(rangeKey, valueBytes);
	}

	@Override
	public List<V> getInternal() {
		byte[] rangeKey = serializeRangeKeyWithNamespace();
		byte[] rawValueBytes = cacheClient.get(keyGroupId(), rangeKey);
		return deserializeList(rawValueBytes);
	}

	@Override
	public void updateInternal(List<V> values) throws Exception {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			byte[] rangeKey = serializeRangeKeyWithNamespace();
			byte[] valueBytes = serializeValueList(values, elementSerializer, DELIMITER);
			cacheClient.update(keyGroupId(), rangeKey, valueBytes, getTtlTime());
		} else {
			clear();
		}
	}

	@Override
	public void update(List<V> values) throws Exception {
		updateInternal(values);
	}

	@Override
	public void addAll(List<V> values) throws Exception {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			byte[] rangeKey = serializeRangeKeyWithNamespace();
			byte[] valueBytes = serializeValueList(values, elementSerializer, DELIMITER);
			mergeValue(rangeKey, valueBytes);
		}
	}

	@Override
	public void clear() {
		byte[] rangeKey = serializeRangeKeyWithNamespace();
		cacheClient.delete(keyGroupId(), rangeKey);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		try {
			// create the target full-binary-key
			setCurrentNamespace(target);
			final byte[] targetKey = serializeRangeKeyWithNamespace();
			
			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {
					setCurrentNamespace(source);
					final byte[] sourceKey = serializeRangeKeyWithNamespace();
					byte[] valueBytes = cacheClient.get(keyGroupId(), sourceKey);

					if (valueBytes != null) {
						cacheClient.delete(keyGroupId(), sourceKey);
						mergeValue(targetKey, valueBytes);
					}
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in taishan", e);
		}
	}

	private void mergeValue(byte[] rangeKey, byte[] valueBytes) {
		// todo use taishan append api
		byte[] oldBytes = cacheClient.get(keyGroupId(), rangeKey);

		byte[] finalByte;
		if (oldBytes != null && oldBytes.length != 0) {
			finalByte = concatByteValue(oldBytes, valueBytes);
		} else {
			finalByte = valueBytes;
		}
		cacheClient.update(keyGroupId(), rangeKey, finalByte, getTtlTime());
	}

	private List<V> deserializeList(byte[] valueBytes) {
		if (valueBytes == null) {
			return getDefaultValue();
		}

		dataInputView.setBuffer(valueBytes);

		List<V> result = new ArrayList<>();
		V next;
		while ((next = deserializeNextElement(dataInputView, elementSerializer)) != null) {
			result.add(next);
		}
		return result;
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<List<V>> safeValueSerializer) throws Exception {
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
		// no compress
		return value;
	}

	private static <V> V deserializeNextElement(DataInputDeserializer in, TypeSerializer<V> elementSerializer) {
		try {
			if (in.available() > 0) {
				V element = elementSerializer.deserialize(in);
				if (in.available() > 0) {
					in.readByte();
				}
				return element;
			}
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected list element deserialization failure", e);
		}
		return null;
	}

	private byte[] concatByteValue(byte[] oldValue, byte[] newValue) {
		// oldValue + byte + newValue
		byte[] finalValue = new byte[oldValue.length + newValue.length + 1];
		System.arraycopy(oldValue, 0, finalValue, 0, oldValue.length);
		finalValue[oldValue.length] = DELIMITER;
		System.arraycopy(newValue, 0, finalValue, oldValue.length + 1, newValue.length);
		return finalValue;
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDescriptor,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
		TaishanKeyedStateBackend<K> backend,
		TaishanMetric taishanMetric) {
		return (IS) new TaishanListState<>(
			stateDescriptor.getName(),
			keyValueStateBackendMetaInfo.getNamespaceSerializer(),
			(TypeSerializer<List<SV>>) keyValueStateBackendMetaInfo.getStateSerializer(),
			backend,
			(List<SV>) stateDescriptor.getDefaultValue(),
			stateDescriptor.getTtlConfig(),
			taishanMetric);
	}

	static class StateSnapshotTransformerWrapper<T> implements StateSnapshotTransformer<byte[]> {
		private final StateSnapshotTransformer<T> elementTransformer;
		private final TypeSerializer<T> elementSerializer;
		private final DataOutputSerializer out = new DataOutputSerializer(128);
		private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;

		StateSnapshotTransformerWrapper(StateSnapshotTransformer<T> elementTransformer, TypeSerializer<T> elementSerializer) {
			this.elementTransformer = elementTransformer;
			this.elementSerializer = elementSerializer;
			this.transformStrategy = elementTransformer instanceof CollectionStateSnapshotTransformer ?
				((CollectionStateSnapshotTransformer) elementTransformer).getFilterStrategy() :
				CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
		}

		@Override
		@Nullable
		public byte[] filterOrTransform(@Nullable byte[] value) {
			if (value == null) {
				return null;
			}
			List<T> result = new ArrayList<>();
			DataInputDeserializer in = new DataInputDeserializer(value);
			T next;
			int prevPosition = 0;
			while ((next = deserializeNextElement(in, elementSerializer)) != null) {
				T transformedElement = elementTransformer.filterOrTransform(next);
				if (transformedElement != null) {
					if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
						return Arrays.copyOfRange(value, prevPosition, value.length);
					} else {
						result.add(transformedElement);
					}
				}
				prevPosition = in.getPosition();
			}
			try {
				return result.isEmpty() ? null : serializeValueList(result, elementSerializer, DELIMITER);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to serialize transformed list", e);
			}
		}

		byte[] serializeValueList(
			List<T> valueList,
			TypeSerializer<T> elementSerializer,
			@SuppressWarnings("SameParameterValue") byte delimiter) throws IOException {

			out.clear();
			boolean first = true;

			for (T value : valueList) {
				Preconditions.checkNotNull(value, "You cannot add null to a value list.");

				if (first) {
					first = false;
				} else {
					out.write(delimiter);
				}
				elementSerializer.serialize(value, out);
			}

			return out.getCopyOfBuffer();
		}
	}
}
