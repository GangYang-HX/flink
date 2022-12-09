/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over the key-values. This class
 * is not thread safe.
 *
 */
public class RocksStateKeyValuesIterator<K> implements Iterator<Tuple4<K, byte[], String, byte[]>>, AutoCloseable {

	@Nonnull
	private final RocksIteratorWrapper iterator;

	@Nonnull
	private final String state;

	@Nonnull
	private final TypeSerializer<K> keySerializer;

	private byte[] nextKey;
	private byte[] nextValue;
	private K deserializedKey;
	private final DataInputDeserializer byteArrayDataInputView;
	private final int keyGroupPrefixBytes;
	private final boolean ambiguousKeyPossible;

	public RocksStateKeyValuesIterator(
		@Nonnull RocksIteratorWrapper iterator,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull String state,
		int keyGroupPrefixBytes,
		boolean ambiguousKeyPossible) {
		this.iterator = iterator;
		this.state = state;
		this.keySerializer = keySerializer;
		this.nextKey = null;
		this.nextValue = null;
		this.byteArrayDataInputView = new DataInputDeserializer();
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.ambiguousKeyPossible = ambiguousKeyPossible;
	}

	@Override
	public boolean hasNext() {
		try {
			while (nextKey == null && iterator.isValid()) {

				final byte[] currentKey = iterator.key();
				final byte[] currentValue = iterator.value();

				nextKey = currentKey;
				deserializedKey = deserializeKey(currentKey, byteArrayDataInputView);
				nextValue = currentValue;
				iterator.next();
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
		}
		return nextKey != null;
	}

	@Override
	public Tuple4<K, byte[], String, byte[]> next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Failed to access state [" + state + "]");
		}

		byte[] tmpKey = nextKey;
		byte[] tmpValue = nextValue;
		K tmpDeserializedKey = deserializedKey;
		nextKey = null;
		nextValue = null;
		deserializedKey = null;
		return new Tuple4<>(tmpDeserializedKey, tmpKey, state, tmpValue);
	}

	private K deserializeKey(byte[] keyBytes, DataInputDeserializer readView) throws IOException {
		readView.setBuffer(keyBytes, keyGroupPrefixBytes, keyBytes.length - keyGroupPrefixBytes);
		return RocksDBKeySerializationUtils.readKey(
			keySerializer,
			byteArrayDataInputView,
			ambiguousKeyPossible);
	}

	@Override
	public void close() {
		iterator.close();
	}

}
