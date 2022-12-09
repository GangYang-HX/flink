/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.bili.writer.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author zhouhuidong
 * @version $Id: NewStreamFileSystemTableSink.java, v 0.1 2021-03-31 7:45 下午 zhouhuidong Exp $$
 */
public class NewStreamFileSystemTableSink extends StreamFileSystemSink<Row, NewStreamFileSystemTableSink>
	implements AppendStreamTableSink<Row> {

	private TableSchema tableSchema;

	/**
	 * Construct a file system table sink.
	 *
	 * @param path       directory path of the file system table.
	 * @param properties properties.
	 */
	public NewStreamFileSystemTableSink(
		Path path,
		Map<String, String> properties,
		ArrayList<String> partitionKeys,
		TableSchema tableSchema,
		ObjectIdentifier tableIdentifier
	) {
		super(path, properties, partitionKeys, tableIdentifier);
		this.tableSchema = tableSchema;
	}

	@Override
	public DataType getConsumedDataType() {
		return TableSchemaUtils.getDataType(tableSchema);
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}


	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return null;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return sinkDataStream(dataStream);
	}
}
