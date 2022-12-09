package com.bilibili.bsql.clickhouse.shard.converter;

import com.bilibili.bsql.clickhouse.shard.connection.ClickHouseConnectionProvider;
import com.bilibili.bsql.clickhouse.shard.table.BsqlClickHouseShardDynamicSink;
import com.bilibili.bsql.common.utils.ObjectUtil;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.io.Serializable;
import java.sql.*;

public class ClickHouseRowConverter implements Serializable {
	private static final Logger LOG  = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);
	protected final int[] sqlTypes;
	private final DynamicTableSink.DataStructureConverter converter;


	public ClickHouseRowConverter(final int[] sqlTypes, final DynamicTableSink.DataStructureConverter converter) {
		this.sqlTypes = sqlTypes;
		this.converter = converter;
	}

	public void insertOrIgnore(RowData rowData, PreparedStatement pstmt) throws SQLException {
		Row row = (Row) converter.toExternal(rowData);
		if (sqlTypes == null) {
			LOG.error("type array is null");
			// no types provided
			int index;
			for (index = 0; index < row.getArity(); index++) {
				pstmt.setObject(index + 1, row.getField(index));
			}
		} else {
			int index;
			// types provided
			for (index = 0; index < row.getArity(); index++) {
				// casting values as suggested by
				// http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
				switch (sqlTypes[index]) {
					case Types.NULL:
						pstmt.setNull(index + 1, sqlTypes[index]);
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						pstmt.setBoolean(index + 1,
							ObjectUtil.getBoolean(row.getField(index), false));
						break;
					case Types.CHAR:
					case Types.NCHAR:
					case Types.VARCHAR:
					case Types.LONGVARCHAR:
					case Types.LONGNVARCHAR:
						pstmt.setString(index + 1, ObjectUtil.getString(row.getField(index), ""));
						break;
					case Types.TINYINT:
						pstmt.setByte(index + 1, ObjectUtil.getByte(row.getField(index)));
						break;
					case Types.SMALLINT:
						pstmt.setShort(index + 1, ObjectUtil.getShort(row.getField(index)));
						break;
					case Types.INTEGER:
						pstmt.setInt(index + 1, ObjectUtil.getInteger(row.getField(index), 0));
						break;
					case Types.BIGINT:
						pstmt.setLong(index + 1, ObjectUtil.getLong(row.getField(index), 0L));
						break;
					case Types.REAL:
					case Types.FLOAT:
						pstmt.setFloat(index + 1, ObjectUtil.getFloat(row.getField(index), 0.0f));
						break;
					case Types.DOUBLE:
						pstmt.setDouble(index + 1, ObjectUtil.getDouble(row.getField(index), 0.0));
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						pstmt.setBigDecimal(index + 1,
							ObjectUtil.getBigDecimal(row.getField(index)));
						break;
					case Types.DATE:
						pstmt.setDate(index + 1, ObjectUtil.getDate(row.getField(index)));
						break;
					case Types.TIME:
						pstmt.setTime(index + 1, (Time) row.getField(index));
						break;
					case Types.TIMESTAMP:
						pstmt.setTimestamp(index + 1,
							ObjectUtil.getTimestamp(row.getField(index), new Timestamp(0)));
						break;
					case Types.BINARY:
					case Types.VARBINARY:
					case Types.LONGVARBINARY:
						pstmt.setBytes(index + 1, (byte[]) row.getField(index));
						break;
					case BsqlClickHouseShardDynamicSink.CLICK_STRING_ARRAY:
						pstmt.setArray(index + 1,
							new ClickHouseArray(ClickHouseDataType.String, row.getField(index)));
						break;
					case BsqlClickHouseShardDynamicSink.CLICK_FLOAT_ARRAY:
						pstmt.setArray(index + 1,
							new ClickHouseArray(ClickHouseDataType.Float64, row.getField(index)));
						break;
					case BsqlClickHouseShardDynamicSink.CLICK_INT_ARRAY:
						pstmt.setArray(index + 1, new ClickHouseArray(ClickHouseDataType.UInt64, row.getField(index)));
						break;
					case BsqlClickHouseShardDynamicSink.CLICK_STRING_STRING_MAP:
						pstmt.setObject(index + 1, row.getField(index));
						break;
					default:
						pstmt.setObject(index + 1, row.getField(index));
						LOG.warn(String.format("Unmanaged sql type (%s) for column %s. Best effort approach to set its value: %s.", sqlTypes[index], index + 1, row.getField(index)));
				}
			}
		}
	}

}
