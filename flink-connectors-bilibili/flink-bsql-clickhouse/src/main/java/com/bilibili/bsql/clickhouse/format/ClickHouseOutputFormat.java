package com.bilibili.bsql.clickhouse.format;

import com.bilibili.bsql.common.utils.ObjectUtil;
import com.bilibili.bsql.rdb.format.RetractJDBCOutputFormat;
import com.bilibili.bsql.rdb.table.BsqlRdbDynamicSinkBase;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.io.IOException;
import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className ClickHouseOutputFormat.java
 * @description This is the description of ClickHouseOutputFormat.java
 * @createTime 2020-10-21 14:51:00
 */
public class ClickHouseOutputFormat extends RetractJDBCOutputFormat {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseOutputFormat.class);

	private final DynamicTableSink.DataStructureConverter converter;
	private RateLimiter rateLimiter;

	public ClickHouseOutputFormat(DataType dataType, DynamicTableSink.Context context) {
	    super(dataType,context);
		this.converter = context.createDataStructureConverter(dataType);
	}

	@Override
	public String sinkType() {
		return "ClickHouse";
	}

	@Override
	public void doOpen(int taskNumber, int numTasks) throws IOException {
		super.doOpen(taskNumber, numTasks);
		init();
		if (StringUtils.isNotEmpty(getTps())) {
			double permitsPerSecond = Double.parseDouble(getTps());
			if (permitsPerSecond > 0) {
				rateLimiter = RateLimiter.create(permitsPerSecond);
			}
		}
	}

	public void init() throws IOException {
		try {
			upload.setQueryTimeout(300);
		} catch (SQLException e) {
			throw new IllegalArgumentException("open() failed.", e);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insertOrIgnore(RowData rowData, PreparedStatement pstmt) throws SQLException {
		Row row = (Row) converter.toExternal(rowData);
		if (typesArray == null) {
			LOGGER.error("type array is null");
			// no types provided
			int index;
			for (index = 0; index < row.getArity(); index++) {
				pstmt.setObject(index + 1, row.getField(index));
			}
			for (int i = 0; i < noPrimaryKeysIndex.size(); i++) {
				pstmt.setObject(++index, row.getField(noPrimaryKeysIndex.get(i)));
			}
		} else {
			int index;
			// types provided
			for (index = 0; index < row.getArity(); index++) {
				// casting values as suggested by
				// http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
				switch (typesArray[index]) {
					case Types.NULL:
						pstmt.setNull(index + 1, typesArray[index]);
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
					case BsqlRdbDynamicSinkBase.CLICK_STRING_ARRAY:
						pstmt.setArray(index + 1,
							new ClickHouseArray(ClickHouseDataType.String, row.getField(index)));
						break;
					case BsqlRdbDynamicSinkBase.CLICK_FLOAT_ARRAY:
						pstmt.setArray(index + 1,
							new ClickHouseArray(ClickHouseDataType.Float64, row.getField(index)));
						break;
					case BsqlRdbDynamicSinkBase.CLICK_INT_ARRAY:
						pstmt.setArray(index + 1, new ClickHouseArray(ClickHouseDataType.UInt64, row.getField(index)));
						break;
					default:
						pstmt.setObject(index + 1, row.getField(index));
						LOGGER.warn(String.format("Unmanaged sql type (%s) for column %s. Best effort approach to set its value: %s.", typesArray[index], index + 1, row.getField(index)));
				}
			}
		}
	}

	@Override
	public void updateOrInsertPreparedStmt(RowData row, PreparedStatement pstmt) throws SQLException {
		// 开启限流
		if (rateLimiter != null) {
			rateLimiter.acquire();
		}
		insertOrIgnore(row, pstmt);
	}
}
