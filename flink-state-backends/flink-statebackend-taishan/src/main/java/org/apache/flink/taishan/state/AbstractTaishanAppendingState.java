package org.apache.flink.taishan.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

/**
 * @author Dove
 * @Date 2022/8/1 3:33 下午
 */
abstract class AbstractTaishanAppendingState<K, N, IN, SV, OUT> extends AbstractTaishanState<K, N, SV> implements InternalAppendingState<K, N, IN, SV, OUT> {
	/**
	 * Creates a new Taishan backend appending state.
	 *
	 * @param stateName           Taishan state name
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer     The serializer for the state.
	 * @param backend             The backend for which this state is bind to.
	 * @param defaultValue        The default value for the state.
	 * @param stateTtlConfig      ttl config
	 * @param taishanMetric       metric group
	 */
	protected AbstractTaishanAppendingState(String stateName,
											TypeSerializer<N> namespaceSerializer,
											TypeSerializer<SV> valueSerializer,
											TaishanKeyedStateBackend<K> backend,
											SV defaultValue,
											StateTtlConfig stateTtlConfig,
											TaishanMetric taishanMetric) {
		super(stateName, namespaceSerializer, valueSerializer, backend, defaultValue, stateTtlConfig, taishanMetric);
	}


	@Override
	public SV getInternal() {
		return getInternal(getKeyBytes());
	}

	SV getInternal(byte[] key) {
		try {
			if (!bloomFilter.mightContains(keyGroupId(), key)) {
				return null;
			}
			byte[] valueBytes = cacheClient.get(keyGroupId(), key);

			if (valueBytes == null) {
				return null;
			}
			valueBytes = decompress(valueBytes);
			dataInputView.setBuffer(valueBytes);
			return valueSerializer.deserialize(dataInputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while retrieving data from Taishan", e);
		}
	}

	@Override
	public void updateInternal(SV valueToStore) {
		updateInternal(getKeyBytes(), valueToStore);
	}

	void updateInternal(byte[] key, SV valueToStore) {
		byte[] valueBytes = getValueBytes(valueToStore);
		byte[] compressValueBytes = compress(valueBytes);
		int ttlTime = getTtlTime();
		this.cacheClient.update(keyGroupId(), key, compressValueBytes, ttlTime);
		bloomFilter.add(keyGroupId(), key, ttlTime);
	}
}
