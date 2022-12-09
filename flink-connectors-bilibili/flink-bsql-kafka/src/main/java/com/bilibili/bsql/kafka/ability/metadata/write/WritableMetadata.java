package com.bilibili.bsql.kafka.ability.metadata.write;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.common.header.Header;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public enum WritableMetadata {

	HEADERS(
		"headers",
		// key and value of the map are nullable to make handling easier in queries
		DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
			.nullable(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(RowData row, int pos) {
				if (row.isNullAt(pos)) {
					return null;
				}
				final MapData map = row.getMap(pos);
				final ArrayData keyArray = map.keyArray();
				final ArrayData valueArray = map.valueArray();
				final List<Header> headers = new ArrayList<>();
				for (int i = 0; i < keyArray.size(); i++) {
					if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
						final String key = keyArray.getString(i).toString();
						final byte[] value = valueArray.getBinary(i);
						headers.add(new KafkaHeader(key, value));
					}
				}
				return headers;
			}
		}),

	TIMESTAMP(
		"timestamp",
		DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(RowData row, int pos) {
				if (row.isNullAt(pos)) {
					return null;
				}
				return row.getTimestamp(pos, 3).getMillisecond();
			}
		});

	public final String key;

	public final DataType dataType;

	public final MetadataConverter converter;

	WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
		this.key = key;
		this.dataType = dataType;
		this.converter = converter;
	}

	public interface MetadataConverter extends Serializable {
		Object read(RowData consumedRow, int pos);
	}

	private static class KafkaHeader implements Header {

		private final String key;

		private final byte[] value;

		KafkaHeader(String key, byte[] value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String key() {
			return key;
		}

		@Override
		public byte[] value() {
			return value;
		}
	}
}
