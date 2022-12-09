package org.apache.flink.taishan.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

public class TaishanValueState<K, N, V>
	extends AbstractTaishanState<K, N, V>
	implements InternalValueState<K, N, V> {

	public TaishanValueState(String stateName,
							 TypeSerializer<N> namespaceSerializer,
							 TypeSerializer<V> valueSerializer,
							 TaishanKeyedStateBackend<K> backend,
							 V defaultValue,
							 StateTtlConfig stateTtlConfig,
							 TaishanMetric taishanMetric) {
		super(stateName,
			namespaceSerializer,
			valueSerializer,
			backend,
			defaultValue,
			stateTtlConfig,
			taishanMetric);
	}

	@Override
	public V value() throws IOException {
		byte[] rangeKey = serializeRangeKeyWithNamespace();
		if (!bloomFilter.mightContains(keyGroupId(), rangeKey)) {
			return getDefaultValue();
		}

		byte[] valueBytes = cacheClient.get(keyGroupId(), rangeKey);

		if (valueBytes == null) {
			return getDefaultValue();
		}
		valueBytes = decompress(valueBytes);
		dataInputView.setBuffer(valueBytes);
		return valueSerializer.deserialize(dataInputView);
	}

	@Override
	public void update(V value) throws IOException {
		if (value == null) {
			clear();
			return;
		}

		try {
			byte[] rangeKey = serializeRangeKeyWithNamespace();
			byte[] valueBytes = serializeValue(value, valueSerializer);
			byte[] compressValueBytes = compress(valueBytes);
			int ttlTime = getTtlTime();
			this.cacheClient.update(keyGroupId(), rangeKey, compressValueBytes, ttlTime);
			bloomFilter.add(keyGroupId(), rangeKey, ttlTime);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to TaiShan", e);
		}
	}

	public static <IS extends S, SV, S extends State, N, K> IS create(
		StateDescriptor<S, SV> stateDescriptor,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> keyValueStateBackendMetaInfo,
		TaishanKeyedStateBackend<K> backend,
		TaishanMetric taishanMetric) {
		return (IS) new TaishanValueState<K, N, SV>(
			stateDescriptor.getName(),
			keyValueStateBackendMetaInfo.getNamespaceSerializer(),
			keyValueStateBackendMetaInfo.getStateSerializer(),
			backend,
			stateDescriptor.getDefaultValue(),
			stateDescriptor.getTtlConfig(),
			taishanMetric
		);
	}
}
