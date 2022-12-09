package org.apache.flink.taishan.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;


/**
 * An {@link AggregatingState} implementation that stores state in Taishan.
 *
 * @param <K>   The type of the key
 * @param <N>   The type of the namespace
 * @param <T>   The type of the values that aggregated into the state
 * @param <ACC> The type of the value stored in the state (the accumulator type)
 * @param <R>   The type of the value returned from the state
 * @author Dove
 * @Date 2022/8/1 4:43 下午
 */
public class TaishanAggregatingState<K, N, T, ACC, R>
	extends AbstractTaishanAppendingState<K, N, T, ACC, R>
	implements InternalAggregatingState<K, N, T, ACC, R> {

	/**
	 * User-specified aggregation function.
	 */
	private final AggregateFunction<T, ACC, R> aggFunction;

	/**
	 * Creates a new Taishan backend Aggregating state.
	 *
	 * @param stateName           Taishan state name
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer     The serializer for the state.
	 * @param backend             The backend for which this state is bind to.
	 * @param defaultValue        The default value for the state.
	 * @param stateTtlConfig      ttl config
	 * @param taishanMetric       metric group
	 */
	protected TaishanAggregatingState(String stateName,
									  TypeSerializer<N> namespaceSerializer,
									  TypeSerializer<ACC> valueSerializer,
									  TaishanKeyedStateBackend<K> backend,
									  ACC defaultValue,
									  AggregateFunction<T, ACC, R> aggFunction,
									  StateTtlConfig stateTtlConfig,
									  TaishanMetric taishanMetric) {
		super(stateName, namespaceSerializer, valueSerializer, backend, defaultValue, stateTtlConfig, taishanMetric);
		this.aggFunction = aggFunction;
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
	public TypeSerializer<ACC> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public R get() {
		ACC accumulator = getInternal();
		if (accumulator == null) {
			return null;
		}
		return aggFunction.getResult(accumulator);
	}

	@Override
	public void add(T value) {
		byte[] key = getKeyBytes();
		ACC accumulator = getInternal(key);
		accumulator = accumulator == null ? aggFunction.createAccumulator() : accumulator;
		updateInternal(key, aggFunction.add(value, accumulator));
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		try {
			ACC current = null;

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
						ACC value = valueSerializer.deserialize(dataInputView);

						if (current != null) {
							current = aggFunction.merge(current, value);
						} else {
							current = value;
						}
					}
				}
			}

			// if something came out of merging the sources, merge it or write it to the target
			if (current != null) {
				setCurrentNamespace(target);
				// create the target full-binary-key
				final byte[] targetKey = serializeRangeKeyWithNamespace();
				if (bloomFilter.mightContains(keyGroupId(), targetKey)) {
					final byte[] targetValueBytes = cacheClient.get(keyGroupId(), targetKey);
					if (targetValueBytes != null) {
						final byte[] decompressTargetValueBytes = decompress(targetValueBytes);
						// target also had a value, merge
						dataInputView.setBuffer(decompressTargetValueBytes);
						ACC value = valueSerializer.deserialize(dataInputView);
						current = aggFunction.merge(current, value);
					}
				}

				// serialize the resulting value
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
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDescriptor,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
		TaishanKeyedStateBackend<K> backend,
		TaishanMetric taishanMetric) {
		return (IS) new TaishanAggregatingState<>(
			stateDescriptor.getName(),
			keyValueStateBackendMetaInfo.getNamespaceSerializer(),
			keyValueStateBackendMetaInfo.getStateSerializer(),
			backend,
			stateDescriptor.getDefaultValue(),
			((AggregatingStateDescriptor<?, SV, ?>) stateDescriptor).getAggregateFunction(),
			stateDescriptor.getTtlConfig(),
			taishanMetric);
	}
}
