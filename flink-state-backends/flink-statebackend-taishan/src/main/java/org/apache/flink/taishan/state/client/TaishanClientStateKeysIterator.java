package org.apache.flink.taishan.state.client;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.taishan.state.serialize.TaishanKeySerializationUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Adapter class to bridge between {@link TaishanClientIteratorWrapper} and {@link Iterator} to iterate over the keys. This class
 * is not thread safe.
 *
 * @param <K> the type of the iterated objects, which are keys in Taishan.
 * @author Dove
 * @Date 2022/8/3 4:44 下午
 */
public class TaishanClientStateKeysIterator<K> implements Iterator<K>, AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanClientStateKeysIterator.class);

	@Nonnull
	private final TaishanClientIteratorWrapper iterator;

	@Nonnull
	private final String state;

	@Nonnull
	private final TypeSerializer<K> keySerializer;

	@Nonnull
	private final byte[] namespaceBytes;

	private final boolean ambiguousKeyPossible;
	private final int keyGroupPrefixBytes;
	private final DataInputDeserializer byteArrayDataInputView;
	private K nextKey;
	private K previousKey;
	private int columnFamilyLength;

	public TaishanClientStateKeysIterator(
		@Nonnull TaishanClientIteratorWrapper iterator,
		@Nonnull String state,
		@Nonnull TypeSerializer<K> keySerializer,
		int keyGroupPrefixBytes,
		boolean ambiguousKeyPossible,
		@Nonnull byte[] namespaceBytes,
		int columnFamilyLength) {
		this.iterator = iterator;
		this.state = state;
		this.keySerializer = keySerializer;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.namespaceBytes = namespaceBytes;
		this.nextKey = null;
		this.previousKey = null;
		this.ambiguousKeyPossible = ambiguousKeyPossible;
		this.byteArrayDataInputView = new DataInputDeserializer();
		this.columnFamilyLength = columnFamilyLength;
	}

	@Override
	public boolean hasNext() {
		try {
			while (nextKey == null && iterator.hasNext()) {
				iterator.next();

				final byte[] keyBytes = iterator.key();
				final K currentKey = deserializeKey(keyBytes, byteArrayDataInputView);
				final int namespaceByteStartPos = byteArrayDataInputView.getPosition();

				if (isMatchingNameSpace(keyBytes, namespaceByteStartPos) && !Objects.equals(previousKey, currentKey)) {
					previousKey = currentKey;
					nextKey = currentKey;
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
		}
		return nextKey != null;
	}

	@Override
	public K next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Failed to access state [" + state + "]");
		}

		K tmpKey = nextKey;
		LOG.debug("nextKey:{}", tmpKey);
		nextKey = null;
		return tmpKey;
	}

	private K deserializeKey(byte[] keyBytes, DataInputDeserializer readView) throws IOException {
		// #ColumnFamily#Key#Namespace#UserKey.
		readView.setBuffer(keyBytes, columnFamilyLength, keyBytes.length - columnFamilyLength);
		return TaishanKeySerializationUtils.readKey(
			keySerializer,
			byteArrayDataInputView,
			ambiguousKeyPossible);
	}

	private boolean isMatchingNameSpace(@Nonnull byte[] key, int beginPos) {
		final int namespaceBytesLength = namespaceBytes.length;
		final int basicLength = namespaceBytesLength + beginPos;
		if (key.length >= basicLength) {
			for (int i = 0; i < namespaceBytesLength; ++i) {
				if (key[beginPos + i] != namespaceBytes[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public void close() {
//		iterator.close();
	}
}
