package org.apache.flink.taishan.state;


import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

/**
 * {@link FoldingState} implementation that stores state in Taishan.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 * @author Dove
 * @Date 2022/8/1 4:53 下午
 * @deprecated will be removed in a future version
 */
public class TaishanFoldingState<K, N, T, ACC>
	extends AbstractTaishanAppendingState<K, N, T, ACC, ACC>
	implements InternalFoldingState<K, N, T, ACC> {
	/** User-specified fold function. */
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new Taishan backend appending state.
	 *
	 * @param stateName Taishan state name
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param backend The backend for which this state is bind to.
	 * @param defaultValue The default value for the state.
	 * @param foldFunction The fold function used for folding state.
	 * @param stateTtlConfig ttl config
	 * @param taishanMetric metric group
	 */
	protected TaishanFoldingState(String stateName,
								  TypeSerializer<N> namespaceSerializer,
								  TypeSerializer<ACC> valueSerializer,
								  TaishanKeyedStateBackend<K> backend,
								  ACC defaultValue,
								  FoldFunction<T, ACC> foldFunction,
								  StateTtlConfig stateTtlConfig,
								  TaishanMetric taishanMetric) {
		super(stateName, namespaceSerializer, valueSerializer, backend, defaultValue, stateTtlConfig, taishanMetric);
		this.foldFunction = foldFunction;
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
	public ACC get() {
		return getInternal();
	}

	@Override
	public void add(T value) throws Exception {
		byte[] key = getKeyBytes();
		ACC accumulator = getInternal(key);
		accumulator = accumulator == null ? getDefaultValue() : accumulator;
		accumulator = foldFunction.fold(accumulator, value);
		updateInternal(key, accumulator);
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDescriptor,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
		TaishanKeyedStateBackend<K> backend,
		TaishanMetric taishanMetric) {
		return (IS) new TaishanFoldingState<>(
			stateDescriptor.getName(),
			keyValueStateBackendMetaInfo.getNamespaceSerializer(),
			keyValueStateBackendMetaInfo.getStateSerializer(),
			backend,
			stateDescriptor.getDefaultValue(),
			((FoldingStateDescriptor<?, SV>) stateDescriptor).getFoldFunction(),
			stateDescriptor.getTtlConfig(),
			taishanMetric);
	}
}
