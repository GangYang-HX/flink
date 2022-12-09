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
 * @author: zhuzhengjun
 * @date: 2022/3/3 3:24 下午
 */
public class MultiStreamFileSystemTableSink extends MultiStreamFileSystemSink<Row, MultiStreamFileSystemTableSink>
	implements AppendStreamTableSink<Row> {

	private final TableSchema tableSchema;

	/**
	 * Construct a file system table sink.
	 *
	 * @param path          directory path of the file system table.
	 * @param properties    properties.
	 * @param partitionKeys partitionKeys
	 * @param tableSchema   tableSchema
	 */
	public MultiStreamFileSystemTableSink(Path path, Map<String, String> properties, ArrayList<String>
		partitionKeys, TableSchema tableSchema, Map<String, ObjectIdentifier> objectIdentifier) {
		super(path, properties, partitionKeys, objectIdentifier);
		this.tableSchema = tableSchema;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return super.multiSinkDataStream(dataStream);
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


}
