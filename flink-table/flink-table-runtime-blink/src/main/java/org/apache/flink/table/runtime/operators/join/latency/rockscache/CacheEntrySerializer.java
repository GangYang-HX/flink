/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency.rockscache;

import java.io.IOException;
import java.util.Objects;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RecordCache.java, v 0.1 2021-02-04 17:11
zhouxiaogang Exp $$
 */
public class CacheEntrySerializer extends TypeSerializer<CacheEntry> {
	private final TypeSerializer<RowData> leftSerializer;
	private final TypeSerializer<RowData> rightSerializer;
	private final TypeSerializer<RowData> outputSerializer;

	public CacheEntrySerializer(TypeSerializer<RowData> leftSerializer,
								TypeSerializer<RowData> rightSerializer,
								TypeSerializer<RowData> outputSerializer) {
		this.leftSerializer = leftSerializer;
		this.rightSerializer = rightSerializer;
		this.outputSerializer = outputSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<CacheEntry> duplicate() {
		return new CacheEntrySerializer(
			leftSerializer.duplicate(),
			rightSerializer.duplicate(),
			outputSerializer.duplicate()
		);
	}

	@Override
	public CacheEntry createInstance() {
		return null;
	}

	@Override
	public CacheEntry copy(CacheEntry from) {
		return new CacheEntry(from.row, from.entryType, from.timestamp);
	}

	@Override
	public CacheEntry copy(CacheEntry from, CacheEntry reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(CacheEntry record, DataOutputView target) throws IOException {
		target.writeInt(record.entryType.ordinal());
		target.writeLong(record.timestamp);
		switch (record.entryType) {
			case OUTPUT:
				outputSerializer.serialize(record.row, target);
				break;
			case LEFT_INPUT:
				leftSerializer.serialize(record.row, target);
				break;
			case RIGHT_INPUT:
				rightSerializer.serialize(record.row, target);
				break;
		}
	}

	@Override
	public CacheEntry deserialize(DataInputView source) throws IOException {

		CacheEntryType cacheEntryType = CacheEntryType.valueOf(source.readInt());
		long timestamp = source.readLong();
		RowData element = null;

		switch (cacheEntryType) {
			case OUTPUT:
				element = outputSerializer.deserialize(source);
				break;
			case LEFT_INPUT:
				element = leftSerializer.deserialize(source);
				break;
			case RIGHT_INPUT:
				element = rightSerializer.deserialize(source);
				break;
		}

		return new CacheEntry(element, cacheEntryType, timestamp);
	}

	@Override
	public CacheEntry deserialize(CacheEntry reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		CacheEntry entry = deserialize(source);
		serialize(entry, target);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CacheEntrySerializer that = (CacheEntrySerializer) o;
		return
			Objects.equals(leftSerializer, that.leftSerializer) &&
			Objects.equals(rightSerializer, that.rightSerializer) &&
			Objects.equals(outputSerializer, that.outputSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(outputSerializer);
	}

	@Override
	public TypeSerializerSnapshot<CacheEntry> snapshotConfiguration() {
		return new CacheEntrySerializerSnapshot(this);
	}

	public static final class CacheEntrySerializerSnapshot
		extends CompositeTypeSerializerSnapshot<CacheEntry, CacheEntrySerializer> {

		private static final int VERSION = 2;

		@SuppressWarnings({"unused", "WeakerAccess"})
		public CacheEntrySerializerSnapshot() {
			super(CacheEntrySerializer.class);
		}

		CacheEntrySerializerSnapshot(CacheEntrySerializer serializerInstance) {
			super(serializerInstance);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return VERSION;
		}

		@Override
		protected TypeSerializer[] getNestedSerializers(CacheEntrySerializer outerSerializer) {
			return new TypeSerializer[]{
				outerSerializer.leftSerializer,
				outerSerializer.rightSerializer,
				outerSerializer.outputSerializer,
			};
		}

		@Override
		@SuppressWarnings("unchecked")
		protected CacheEntrySerializer createOuterSerializerWithNestedSerializers(TypeSerializer[] nestedSerializers) {
			return new CacheEntrySerializer(
				nestedSerializers[0],
				nestedSerializers[1],
				nestedSerializers[2]
			);
		}
	}
}
