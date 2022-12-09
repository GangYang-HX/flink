/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import com.github.luben.zstd.Zstd;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>State is not stored in this class but in the {@link org.rocksdb.RocksDB} instance that
 * the {@link RocksDBStateBackend} manages and checkpoints.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of values kept internally in state.
 */
public abstract class AbstractRocksDBState<K, N, V> implements InternalKvState<K, N, V>, State {

	/** Serializer for the namespace. */
	final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state values. */
	final TypeSerializer<V> valueSerializer;

	/** The current namespace, which the next value methods will refer to. */
	private N currentNamespace;

	/** Backend that holds the actual RocksDB instance where we store state. */
	protected RocksDBKeyedStateBackend<K> backend;

	/** The column family of this particular instance of state. */
	protected ColumnFamilyHandle columnFamily;

	protected final V defaultValue;

	protected final WriteOptions writeOptions;

	protected final DataOutputSerializer dataOutputView;

	protected final DataInputDeserializer dataInputView;

	protected final MetricGroup metricGroup;

	protected final Histogram writeHist;

	/** The lenth of the keys in rocksdb of the key */
	protected final Histogram writeKeyLength;

	/** The lenth of the values in rocksdb of the key */
	protected final Histogram writeValueLength;

	protected final Counter writeCounter;

	protected final Histogram deleteHist;

	protected final Histogram deleteKeyLength;

	protected final Counter deleteCounter;

	protected final Histogram readHist;

	/** The cost of rocksdb when there is null. */
	protected final Histogram readNull;

	/** The lenth of the keys in rocksdb of the key */
	protected final Histogram readKeyLength;

	/** The lenth of the values in rocksdb of the key */
	protected final Histogram readValueLength;

	/** The lenth of the values in rocksdb of the key */
	protected final Counter readCounter;

	protected final Counter readNullCounter;

	/** The cost of rocksdb when iterator seek. */
	protected final Histogram seekHist;

	protected final Histogram seekKeyLength;

	/** The lenth of the values in rocksdb of the key */
	protected final Histogram seekValueLength;

	protected final Counter seekCounter;

	/** The cost of rocksdb when iterator next. */
	protected final Histogram nextHist;

	protected final Counter nextCounter;

	/** The lenth of the values in rocksdb of the key */
	protected final Histogram nextValueLength;

	/** The controler of the catching rate of rocksdb's cost */
	protected long loopLock;

	protected boolean catchSample;

	protected int samplingRate;

	protected boolean compressEnabled;

	private final RocksDBSerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

	private final Boolean isTtlState;

	/**
	 * Creates a new RocksDB backed state.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param backend The backend for which this state is bind to.
	 */
	protected AbstractRocksDBState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			RocksDBKeyedStateBackend<K> backend,
			MetricGroup metricGroup) {
		this.namespaceSerializer = namespaceSerializer;
		this.backend = backend;

		this.columnFamily = columnFamily;

		this.writeOptions = backend.getWriteOptions();
		this.valueSerializer = Preconditions.checkNotNull(valueSerializer, "State value serializer");
		this.defaultValue = defaultValue;

		this.dataOutputView = new DataOutputSerializer(128);
		this.dataInputView = new DataInputDeserializer();
		this.sharedKeyNamespaceSerializer = backend.getSharedRocksKeyBuilder();

		if (valueSerializer instanceof MapSerializer) {
			isTtlState = ((MapSerializer<?, ?>) valueSerializer).getValueSerializer() instanceof TtlStateFactory.TtlSerializer;
		}
		else {
			isTtlState = valueSerializer instanceof TtlStateFactory.TtlSerializer;
		}

		this.metricGroup = metricGroup;
		this.writeHist = metricGroup.histogram("writeHist", new DescriptiveStatisticsHistogram(1000));
		this.writeKeyLength = metricGroup.histogram("writeKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.writeValueLength = metricGroup.histogram("writeValueLength", new DescriptiveStatisticsHistogram(1000));
		this.writeCounter = metricGroup.counter("writeCounter");
		this.deleteHist = metricGroup.histogram("deleteHist", new DescriptiveStatisticsHistogram(1000));
		this.deleteKeyLength = metricGroup.histogram("deleteKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.deleteCounter = metricGroup.counter("deleteCounter");
		this.readHist = metricGroup.histogram("readHist", new DescriptiveStatisticsHistogram(1000));
		this.readNull = metricGroup.histogram("readNull", new DescriptiveStatisticsHistogram(1000));
		this.readKeyLength = metricGroup.histogram("readKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.readValueLength = metricGroup.histogram("readValueLength", new DescriptiveStatisticsHistogram(1000));
		this.readCounter = metricGroup.counter("readCounter");
		this.readNullCounter = metricGroup.counter("readNullCounter");
		this.seekHist = metricGroup.histogram("seekHist", new DescriptiveStatisticsHistogram(1000));
		this.seekKeyLength = metricGroup.histogram("seekKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.seekValueLength = metricGroup.histogram("seekValueLength", new DescriptiveStatisticsHistogram(1000));
		this.seekCounter = metricGroup.counter("seekCounter");
		this.nextHist = metricGroup.histogram("nextHist", new DescriptiveStatisticsHistogram(1000));
		this.nextValueLength = metricGroup.histogram("nextValueLength", new DescriptiveStatisticsHistogram(1000));
		this.nextCounter = metricGroup.counter("nextCounter");
		ExecutionConfig config = backend.getExecutionConfig();
		this.compressEnabled = config.rocksDBCompressEnabled;
		this.samplingRate = config.sampleRate;
		this.loopLock = 0;
	}

	// ------------------------------------------------------------------------

	@Override
	public void clear() {
		try {
			byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespace();
			updateCatchingRate();
			long start = 0;
			if (catchSample) {
				start = System.nanoTime();
			}
			backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
			takeDeleteMetrics(start, rawKeyBytes);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<V> safeValueSerializer) throws Exception {

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(keyAndNamespace.f0, backend.getNumberOfKeyGroups());

		RocksDBSerializedCompositeKeyBuilder<K> keyBuilder =
						new RocksDBSerializedCompositeKeyBuilder<>(
							safeKeySerializer,
							backend.getKeyGroupPrefixBytes(),
							32
						);
		keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
		byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
		return decompress(backend.db.get(columnFamily, key));
	}

	<UK> byte[] serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
		UK userKey,
		TypeSerializer<UK> userKeySerializer) throws IOException {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamesSpaceUserKey(
			currentNamespace,
			namespaceSerializer,
			userKey,
			userKeySerializer
		);
	}

	private <T> byte[] serializeValueInternal(T value, TypeSerializer<T> serializer) throws IOException {
		serializer.serialize(value, dataOutputView);
		return dataOutputView.getCopyOfBuffer();
	}

	byte[] serializeCurrentKeyWithGroupAndNamespace() {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(currentNamespace, namespaceSerializer);
	}

	byte[] serializeValue(V value) throws IOException {
		return serializeValue(value, valueSerializer);
	}

	<T> byte[] serializeValueNullSensitive(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		dataOutputView.writeBoolean(value == null);
		return serializeValueInternal(value, serializer);
	}

	<T> byte[] serializeValue(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
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

	public void migrateSerializedValue(
			DataInputDeserializer serializedOldValueInput,
			DataOutputSerializer serializedMigratedValueOutput,
			TypeSerializer<V> priorSerializer,
			TypeSerializer<V> newSerializer) throws StateMigrationException {

		try {
			V value = priorSerializer.deserialize(serializedOldValueInput);
			newSerializer.serialize(value, serializedMigratedValueOutput);
		} catch (Exception e) {
			throw new StateMigrationException("Error while trying to migrate RocksDB state.", e);
		}
	}

	byte[] getKeyBytes() {
		return serializeCurrentKeyWithGroupAndNamespace();
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

	protected V getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}

	@Override
	public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		throw new UnsupportedOperationException("Global state entry iterator is unsupported for RocksDb backend");
	}

	protected void updateCatchingRate() {
		int x = 100 - samplingRate;
		catchSample = (loopLock + x) % 100 >= x;
	}

	protected void takeReadMetrics(long start, byte[] key,@Nullable byte[] value) {
		if (value!=null) {
			if (catchSample) {
				long end = System.nanoTime();
				this.readHist.update(end - start);
				this.readKeyLength.update(key.length);
				this.readValueLength.update(value.length);
			}
			this.readCounter.inc();
		}
		else{
			if (catchSample) {
				long end = System.nanoTime();
				this.readNull.update(end - start);
			}
			this.readNullCounter.inc();
		}
		this.loopLock++;
	}

	protected void takeWriteMetrics(long start, byte[] key,@Nullable byte[] value) {
		if (catchSample) {
			long end = System.nanoTime();
			this.writeHist.update(end - start);
			this.writeKeyLength.update(key.length);
			if (value != null) {
				this.writeValueLength.update(value.length);
			}
		}
		this.writeCounter.inc();
		this.loopLock++;
	}

	protected void takeDeleteMetrics(long start, byte[] key) {
		if (catchSample) {
			long end = System.nanoTime();
			this.deleteHist.update(end - start);
			this.deleteKeyLength.update(key.length);
		}
		this.deleteCounter.inc();
		this.loopLock++;
	}

	protected void takeSeekMetrics(long start, byte[] key, RocksIteratorWrapper iter) {
		if (catchSample) {
			long end = System.nanoTime();
			this.seekHist.update(end - start);
			this.seekKeyLength.update(key.length);
			if (iter.isValid()) {
				this.seekValueLength.update(iter.value().length);
			}
		}
		this.seekCounter.inc();
		this.loopLock++;
	}

	protected void takeNextMetrics(long start, RocksIteratorWrapper iter) {
		if (catchSample) {
			long end = System.nanoTime();
			this.seekHist.update(end - start);
			if (iter.isValid()) {
				this.nextValueLength.update(iter.value().length);
			}
		}
		this.nextCounter.inc();
		this.loopLock++;
	}

	protected byte[] compress(byte[] val) {
		if (compressEnabled && val != null) {
			if (isTtlState) {
				byte[] timeStamp = Arrays.copyOfRange(val,0,8);
				val = com.github.luben.zstd.Zstd.compress(Arrays.copyOfRange(val,8,val.length), 3);
				return ArrayUtils.concat(timeStamp,val);
			}
			else {
				return com.github.luben.zstd.Zstd.compress(val);
			}
		}

		return val;
	}

	protected byte[] decompress(byte[] val) {
		if (compressEnabled && val != null && !(this instanceof RocksDBListState)) {
			if (isTtlState) {
				byte[] rawVal = Arrays.copyOfRange(val,8,val.length);
				int length = (int) Zstd.decompressedSize(rawVal);
				rawVal = Zstd.decompress(rawVal, length);
				return ArrayUtils.concat(Arrays.copyOfRange(val,0,8), rawVal);
			}
			else {
				int length = (int) Zstd.decompressedSize(val);
				return Zstd.decompress(val, length);
			}
		}
		return val;
	}
}
