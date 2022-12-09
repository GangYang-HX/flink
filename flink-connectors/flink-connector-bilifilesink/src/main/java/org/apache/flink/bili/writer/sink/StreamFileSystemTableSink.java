/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.sink;

import java.util.ArrayList;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 *
 * @author zhouxiaogang
 * @version $Id: FileSystemTableSink.java, v 0.1 2020-05-31 17:03
zhouxiaogang Exp $$
 */
public class StreamFileSystemTableSink extends StreamFileSystemSink<Row, StreamFileSystemTableSink>
	implements AppendStreamTableSink<Row> {

	private String[]           fieldNames;
	private TypeInformation<?>[] fieldTypes;

	/**
	 * Construct a file system table sink.
	 *
	 * @param path       directory path of the file system table.
	 * @param properties properties.
	 */
	public StreamFileSystemTableSink(
		Path path,
		Map<String, String> properties,
		ArrayList<String> partitionKeys,
		ObjectIdentifier tableIdentifier
	) {

		super(path, properties, partitionKeys, tableIdentifier);
	}

	@Override
	public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
		this.fieldNames = strings;
		this.fieldTypes = typeInformations;
		return this;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return sinkDataStream(dataStream);
	}
}
