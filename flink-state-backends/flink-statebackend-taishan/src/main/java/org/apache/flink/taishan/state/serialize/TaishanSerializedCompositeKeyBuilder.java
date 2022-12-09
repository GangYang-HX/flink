package org.apache.flink.taishan.state.serialize;


import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * Responsible for serialization of currentKey, currentGroup and namespace.
 * Will reuse the previous serialized currentKeyed if possible.
 *
 * @param <K> type of the key.
 */
@NotThreadSafe
@Internal
public class TaishanSerializedCompositeKeyBuilder<K> {

	/** The serializer for the key. */
	@Nonnull
	private final TypeSerializer<K> keySerializer;

	/** The output to write the key into. */
	@Nonnull
	private final DataOutputSerializer keyOutView;

	/** The number of Key-group-prefix bytes for the key. */
	@Nonnegative
	private final int keyGroupPrefixBytes;

	/** This flag indicates whether the key type has a variable byte size in serialization. */
	private final boolean keySerializerTypeVariableSized;

	/** key */
	private K key;
	/** keyGroupId */
	private int keyGroupId;

	/** Mark for the position after the serialized key. */
	@Nonnegative
	private int afterKeyMark;
	private int afterCFMark;

	/** column family */
	private String stateName;

	public TaishanSerializedCompositeKeyBuilder(
		String stateName,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnegative int initialSize) {
		this(
			stateName,
			keySerializer,
			new DataOutputSerializer(initialSize),
			keyGroupPrefixBytes,
			TaishanKeySerializationUtils.isSerializerTypeVariableSized(keySerializer),
			0);
	}

	@VisibleForTesting
	TaishanSerializedCompositeKeyBuilder(
		String stateName,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull DataOutputSerializer keyOutView,
		@Nonnegative int keyGroupPrefixBytes,
		boolean keySerializerTypeVariableSized,
		@Nonnegative int afterKeyMark) {
		this.stateName = stateName;
		this.keySerializer = keySerializer;
		this.keyOutView = keyOutView;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.keySerializerTypeVariableSized = keySerializerTypeVariableSized;
		this.afterKeyMark = afterKeyMark;

		setColumnFamily(this.stateName);
	}


	/**
	 * Sets the key as prefix. This will serialize them into the buffer and the will be used to create
	 * composite keys with provided namespaces.
	 *
	 * @param key the key.
	 * @param keyGroupId Key Group Id
	 */
	public void setKey(@Nonnull K key, int keyGroupId) {
		this.key = key;
		this.keyGroupId = keyGroupId;
	}

	/**
	 * set columnFamily as prefix before key.
	 *
	 * @param stateName
	 */
	public void setColumnFamily(String stateName) {
		try {
			serializeColumnFamily(stateName);
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}

	public int getKeyGroupId() {
		return keyGroupId;
	}

	public int getColumnFamilyLength() {
		return afterCFMark;
	}

	/**
	 * @return #ColumnFamily
	 */
	public byte[] buildCompositeColumnFamily() {
		resetToCF();
		final byte[] result = keyOutView.getCopyOfBuffer();

		return result;
	}

	/**
	 * Returns a serialized composite key, from the columnFamily and key provided in a previous call to
	 * {@link #setColumnFamily(String)}} and the given namespace.
	 *
	 * @param namespace the namespace to concatenate for the serialized composite key bytes.
	 * @param namespaceSerializer the serializer to obtain the serialized form of the namespace.
	 * @param <N> the type of the namespace.
	 * @return #ColumnFamily#Key#Namespace
	 */
	@Nonnull
	public <N> byte[] buildCompositeKeyNamespace(@Nonnull N namespace, @Nonnull TypeSerializer<N> namespaceSerializer) {
		try {
			// todo Optimize key serialization times(Extract the CF)
			serializeKey(key);
			if (namespace != null) {
				serializeNamespace(namespace, namespaceSerializer);
			}
			final byte[] result = keyOutView.getCopyOfBuffer();
			resetToKey();
			return result;
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}


	/**
	 * Returns a serialized composite key, from the key and key-group provided in a previous call to
	 * {@link #setColumnFamily(String)} {@link #setKey(Object, int)} and the given namespace, folloed by the given user-key.
	 *
	 * @param namespace the namespace to concatenate for the serialized composite key bytes.
	 * @param namespaceSerializer the serializer to obtain the serialized form of the namespace.
	 * @param userKey the user-key to concatenate for the serialized composite key, after the namespace.
	 * @param userKeySerializer the serializer to obtain the serialized form of the user-key.
	 * @param <N> the type of the namespace.
	 * @param <UK> the type of the user-key.
	 * @return #ColumnFamily#Key#Namespace#UserKey.
	 */
	@Nonnull
	public <N, UK> byte[] buildCompositeKeyNamesSpaceUserKey(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull UK userKey,
		@Nonnull TypeSerializer<UK> userKeySerializer) throws IOException {
		serializeKey(key);
		serializeNamespace(namespace, namespaceSerializer);
		userKeySerializer.serialize(userKey, keyOutView);
		byte[] result = keyOutView.getCopyOfBuffer();
		resetToKey();
		return result;
	}

	private void serializeColumnFamily(String columnFamily) throws IOException {
		resetFully();

		// write ColumnFamily
		StringSerializer.INSTANCE.serialize(columnFamily, keyOutView);
		afterCFMark = keyOutView.length();
	}

	private void serializeKey(K key) throws IOException {

		// clear buffer and mark
		resetToCF();

		// write key
		keySerializer.serialize(key, keyOutView);
		afterKeyMark = keyOutView.length();
	}

	private <N> void serializeNamespace(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer) throws IOException {

		// this should only be called when there is already a key written so that we build the composite.
		assert isKeyWritten();

		final boolean ambiguousCompositeKeyPossible = isAmbiguousCompositeKeyPossible(namespaceSerializer);
		if (ambiguousCompositeKeyPossible) {
			TaishanKeySerializationUtils.writeVariableIntBytes(
				afterKeyMark - keyGroupPrefixBytes,
				keyOutView);
		}
		TaishanKeySerializationUtils.writeNameSpace(
			namespace,
			namespaceSerializer,
			keyOutView,
			ambiguousCompositeKeyPossible);
	}

	private void resetFully() {
		afterKeyMark = 0;
		keyOutView.clear();
	}

	private void resetToCF() {
		keyOutView.setPosition(afterCFMark);
	}

	private void resetToKey() {
		keyOutView.setPosition(afterKeyMark);
	}

	private boolean isKeyWritten() {
		return afterKeyMark > 0;
	}

	@VisibleForTesting
	boolean isAmbiguousCompositeKeyPossible(TypeSerializer<?> namespaceSerializer) {
		return keySerializerTypeVariableSized &
			TaishanKeySerializationUtils.isSerializerTypeVariableSized(namespaceSerializer);
	}
}
