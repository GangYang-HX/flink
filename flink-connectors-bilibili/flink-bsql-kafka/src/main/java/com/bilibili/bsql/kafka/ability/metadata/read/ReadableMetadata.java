package com.bilibili.bsql.kafka.ability.metadata.read;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public enum ReadableMetadata implements Serializable {
	TOPIC(
		"topic",
		DataTypes.STRING().notNull(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(ConsumerRecord<?, ?> record) {
				return StringData.fromString(record.topic());
			}
		}),

	PARTITION(
		"partition",
		DataTypes.INT().notNull(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(ConsumerRecord<?, ?> record) {
				return record.partition();
			}
		}),

	HEADERS(
		"headers",
		// key and value of the map are nullable to make handling easier in queries
		DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
			.notNull(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(ConsumerRecord<?, ?> record) {
				final Map<StringData, byte[]> map = new HashMap<>();
				for (Header header : record.headers()) {
					map.put(StringData.fromString(header.key()), header.value());
				}
				return new GenericMapData(map);
			}
		}),

	OFFSET(
		"offset",
		DataTypes.BIGINT().notNull(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(ConsumerRecord<?, ?> record) {
				return record.offset();
			}
		}),

	TIMESTAMP(
		"timestamp",
		DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(ConsumerRecord<?, ?> record) {
				return TimestampData.fromEpochMillis(record.timestamp());
			}
		}),

	TIMESTAMP_TYPE(
		"timestamp-type",
		DataTypes.STRING().notNull(),
		new MetadataConverter() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object read(ConsumerRecord<?, ?> record) {
				return StringData.fromString(record.timestampType().toString());
			}
		});

	public final String key;

	public final DataType dataType;

	public final MetadataConverter converter;

	ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
		this.key = key;
		this.dataType = dataType;
		this.converter = converter;
	}

	public interface MetadataConverter extends Serializable {
		Object read(ConsumerRecord<?, ?> record);
	}
}
