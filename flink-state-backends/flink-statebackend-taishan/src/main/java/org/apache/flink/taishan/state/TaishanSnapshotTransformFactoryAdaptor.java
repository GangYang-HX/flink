package org.apache.flink.taishan.state;



import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;

import java.util.Optional;
/**
 * @author Dove
 * @Date 2022/8/2 1:50 下午
 */
abstract class TaishanSnapshotTransformFactoryAdaptor<SV, SEV> implements StateSnapshotTransformFactory<SV> {
	final StateSnapshotTransformFactory<SEV> snapshotTransformFactory;

	TaishanSnapshotTransformFactoryAdaptor(StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		this.snapshotTransformFactory = snapshotTransformFactory;
	}

	@Override
	public Optional<StateSnapshotTransformer<SV>> createForDeserializedState() {
		throw new UnsupportedOperationException("Only serialized state filtering is supported in taishan backend");
	}

	@SuppressWarnings("unchecked")
	static <SV, SEV> StateSnapshotTransformFactory<SV> wrapStateSnapshotTransformFactory(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
		TypeSerializer<SV> stateSerializer) {
		if (stateDesc instanceof ListStateDescriptor) {
			TypeSerializer<SEV> elementSerializer = ((ListSerializer<SEV>) stateSerializer).getElementSerializer();
			return new TaishanListStateSnapshotTransformFactory<>(snapshotTransformFactory, elementSerializer);
		} else if (stateDesc instanceof MapStateDescriptor) {
			return new TaishanMapStateSnapshotTransformFactory<>(snapshotTransformFactory);
		} else {
			return new TaishanValueStateSnapshotTransformFactory<>(snapshotTransformFactory);
		}
	}

	private static class TaishanValueStateSnapshotTransformFactory<SV, SEV>
		extends TaishanSnapshotTransformFactoryAdaptor<SV, SEV> {

		private TaishanValueStateSnapshotTransformFactory(StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
			super(snapshotTransformFactory);
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			return snapshotTransformFactory.createForSerializedState();
		}
	}

	private static class TaishanMapStateSnapshotTransformFactory<SV, SEV>
		extends TaishanSnapshotTransformFactoryAdaptor<SV, SEV> {

		private TaishanMapStateSnapshotTransformFactory(StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
			super(snapshotTransformFactory);
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			return snapshotTransformFactory.createForSerializedState()
				.map(TaishanMapState.StateSnapshotTransformerWrapper::new);
		}
	}

	private static class TaishanListStateSnapshotTransformFactory<SV, SEV>
		extends TaishanSnapshotTransformFactoryAdaptor<SV, SEV> {

		private final TypeSerializer<SEV> elementSerializer;

		@SuppressWarnings("unchecked")
		private TaishanListStateSnapshotTransformFactory(
			StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
			TypeSerializer<SEV> elementSerializer) {

			super(snapshotTransformFactory);
			this.elementSerializer = elementSerializer;
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			return snapshotTransformFactory.createForDeserializedState()
				.map(est -> new TaishanListState.StateSnapshotTransformerWrapper<>(est, elementSerializer.duplicate()));
		}
	}
}
