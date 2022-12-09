package org.apache.flink.taishan.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/**
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 * @author Dove
 * @Date 2022/8/1 3:44 下午
 * {@link ReducingState} implementation that stores state in Taishan.
 */
public class TaishanReducingState<K, N, V>
	extends AbstractTaishanAppendingState<K, N, V, V, V>
	implements InternalReducingState<K, N, V> {
	/** User-specified reduce function. */
	private final ReduceFunction<V> reduceFunction;

	/**
	 * Creates a new Taishan backend reducing state.
	 *
	 * @param stateName Taishan state name
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param backend The backend for which this state is bind to.
	 * @param defaultValue The default value for the state.
	 * @param stateTtlConfig ttl config
	 * @param taishanMetric metric group
	 */
	protected TaishanReducingState(String stateName,
								   TypeSerializer<N> namespaceSerializer,
								   TypeSerializer<V> valueSerializer,
								   TaishanKeyedStateBackend<K> backend,
								   V defaultValue,
								   ReduceFunction<V> reduceFunction,
								   StateTtlConfig stateTtlConfig,
								   TaishanMetric taishanMetric) {
		super(stateName, namespaceSerializer, valueSerializer, backend, defaultValue, stateTtlConfig, taishanMetric);
		this.reduceFunction = reduceFunction;
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
	public V get() {
		return getInternal();
	}

	@Override
	public void add(V value) throws Exception {
		byte[] key = getKeyBytes();
		V oldValue = getInternal(key);
		V newValue = oldValue == null ? value : reduceFunction.reduce(oldValue, value);
		updateInternal(key, newValue);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		try {
			V current = null;

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {
					setCurrentNamespace(source);
					final byte[] sourceKey = serializeRangeKeyWithNamespace();
					if (!bloomFilter.mightContains(keyGroupId(), sourceKey)) {
						continue;
					}

					final byte[] valueBytes = cacheClient.get(keyGroupId(), sourceKey);

					if (valueBytes != null) {
						cacheClient.delete(keyGroupId(), sourceKey);
						bloomFilter.delete(keyGroupId(), sourceKey);
						final byte[] decompressValueBytes = decompress(valueBytes);
						dataInputView.setBuffer(decompressValueBytes);
						V value = valueSerializer.deserialize(dataInputView);
						if (current != null) {
							current = reduceFunction.reduce(current, value);
						} else {
							current = value;
						}
					}
				}
			}

			// if something came out of merging the sources, merge it or write it to the target
			if (current != null) {
				// create the target full-binary-key
				setCurrentNamespace(target);
				final byte[] targetKey = serializeRangeKeyWithNamespace();
				if (bloomFilter.mightContains(keyGroupId(), targetKey)) {
					final byte[] targetValueBytes = this.cacheClient.get(keyGroupId(), targetKey);
					if (targetValueBytes != null) {
						final byte[] decompressTargetValueBytes = decompress(targetValueBytes);
						dataInputView.setBuffer(decompressTargetValueBytes);
						// target also had a value, merge
						V value = valueSerializer.deserialize(dataInputView);
						current = reduceFunction.reduce(current, value);
					}
				}

				dataOutputView.clear();
				valueSerializer.serialize(current, dataOutputView);
				// write the resulting value
				final byte[] targetValue = dataOutputView.getCopyOfBuffer();
				final byte[] compressTargetValue = compress(targetValue);
				int ttlTime = getTtlTime();
				cacheClient.update(keyGroupId(), targetKey, compressTargetValue, ttlTime);
				bloomFilter.add(keyGroupId(), targetKey, ttlTime);
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in Taishan", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDescriptor,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
		TaishanKeyedStateBackend<K> backend,
		TaishanMetric taishanMetric) {
		return (IS) new TaishanReducingState<>(
			stateDescriptor.getName(),
			keyValueStateBackendMetaInfo.getNamespaceSerializer(),
			keyValueStateBackendMetaInfo.getStateSerializer(),
			backend,
			stateDescriptor.getDefaultValue(),
			((ReducingStateDescriptor<SV>) stateDescriptor).getReduceFunction(),
			stateDescriptor.getTtlConfig(),
			taishanMetric);
	}

}
