package com.bilibili.bsql.mysql.format;

import com.bilibili.bsql.common.utils.ObjectUtil;
import com.bilibili.bsql.mysql.cache.Cache;
import com.bilibili.bsql.mysql.cache.CompressionCache;
import com.bilibili.bsql.mysql.cache.NoCompressionCache;
import com.bilibili.bsql.rdb.format.RetractJDBCOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className MysqlOutputFormat.java
 * @description This is the description of MysqlOutputFormat.java
 * @createTime 2020-10-22 14:20:00
 */
public class MysqlOutputFormat extends RetractJDBCOutputFormat {
	private static final Logger LOG = LoggerFactory.getLogger(MysqlOutputFormat.class);

	private final DynamicTableSink.DataStructureConverter converter;
    private boolean enableCompression;
    private Cache dataCache;

	public MysqlOutputFormat(DataType dataType, DynamicTableSink.Context context) {
	    super(dataType,context);
		this.converter = context.createDataStructureConverter(dataType);
	}

    @Override
    public void doOpen(int taskNumber, int numTasks) throws IOException {
        super.doOpen(taskNumber, numTasks);
        if (enableCompression) {
            dataCache = new CompressionCache(taskNumber, primaryKeysIndex, batchMaxCount, converter, insertType);
        } else {
            dataCache = new NoCompressionCache(taskNumber);
        }
        dataCache.validPrimaryKey();
    }

	/** modified to public access for {@link MysqlDeletableOutputFormatWrapper.MysqlJdbcConnectionProvider} */
	@Override
	public Connection establishConnection() throws SQLException, ClassNotFoundException {
		return super.establishConnection();
	}

	/** modified to public access for {@link MysqlDeletableOutputFormatWrapper.MysqlJdbcConnectionProvider} */
	@Override
	public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
		return super.reestablishConnection();
	}

	@Override
	public String sinkType() {
		return "MySql";
	}

	@Override
	protected boolean needValidateTest() {
		return dataCache.isEmpty() && dbConn != null;
	}

	@Override
	protected void validateTest() throws SQLException {
		try {
			dbConn.isValid(2000);
			LOG.info("no data send in a flush, send ping instead");
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

	@Override
	protected void doWrite(RowData record) {
        dataCache.addData(record);
	}

	@Override
	public void insertOrIgnore(RowData rowData, PreparedStatement preparedStatement) throws SQLException {
		Row row = (Row) converter.toExternal(rowData);
		{
			if (typesArray == null) {
				LOG.warn("type array is null");
				int index;
				for (index = 0; index < row.getArity(); index++) {
					preparedStatement.setObject(index + 1, row.getField(index));
				}
			} else {
				int index;
				for (index = 0; index < row.getArity(); index++) {
					typeConvertAndSetVal(preparedStatement, index, typesArray[index], row.getField(index));
				}
			}
		}
	}

	@Override
	public void updateOrInsertPreparedStmt(RowData rowData, PreparedStatement pstmt) throws SQLException {
		Row row = (Row) converter.toExternal(rowData);
		if (typesArray == null) {
			LOG.warn("type array is null");
			int index;
			for (index = 0; index < row.getArity(); index++) {
				pstmt.setObject(index + 1, row.getField(index));
			}
			for (int i = 0; i < noPrimaryKeysIndex.size(); i++) {
				pstmt.setObject(++index, row.getField(noPrimaryKeysIndex.get(i)));
			}
		} else {
			int index;
			for (index = 0; index < row.getArity(); index++) {
				typeConvertAndSetVal(pstmt, index, typesArray[index], row.getField(index));
			}
			for (int i = 0; i < noPrimaryKeysIndex.size(); i++) {
				typeConvertAndSetVal(pstmt, index, typesArray[noPrimaryKeysIndex.get(i)], row.getField(noPrimaryKeysIndex.get(i)));
				index++;
			}
		}
	}

	@Override
	protected void flush() throws Exception {
		for (int i = 0; i <= super.maxRetries; i++) {
			try {
				attemptFlush();
				break;
			} catch (Exception e) {
				LOG.error("JDBC executeBatch error, retry times = {}", i, e);
				sinkMetricsGroup.retryRecord();
				if (i >= maxRetries) {
					throw new IOException(e);
				}
				try {
					if (!super.dbConn.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS)) {
						dbConn = super.reestablishConnection();
						super.upload.close();
						super.upload = dbConn.prepareStatement(super.insertQuery);
						LOG.info("mysql connection was reestablished!");
					}
				} catch (SQLException | ClassNotFoundException throwables) {
					throw new IOException("JDBC connection is not valid, and reestablish connection failed.", throwables);
				}
				try {
					Thread.sleep(1000 * i);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IOException(
						"unable to flush; interrupted while doing another attempt", e);
				}
			}

		}

	}

    private void attemptFlush() throws Exception {
        dataCache.consumeData(rowData -> super.doWrite(rowData));
        super.flush();
        dataCache.clearData();
    }

	private void typeConvertAndSetVal(PreparedStatement preparedStatement, int index, int type, Object value) throws SQLException {
		switch (type) {
			case Types.NULL:
				preparedStatement.setNull(index + 1, type);
				break;
			case Types.BOOLEAN:
			case Types.BIT:
				preparedStatement.setBoolean(index + 1, ObjectUtil.getBoolean(value, false));
				break;
			case Types.CHAR:
			case Types.NCHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.LONGNVARCHAR:
				preparedStatement.setString(index + 1, ObjectUtil.getString(value, ""));
				break;
			case Types.TINYINT:
				preparedStatement.setByte(index + 1, ObjectUtil.getByte(value));
				break;
			case Types.SMALLINT:
				preparedStatement.setShort(index + 1, ObjectUtil.getShort(value));
				break;
			case Types.INTEGER:
				preparedStatement.setInt(index + 1, ObjectUtil.getInteger(value, 0));
				break;
			case Types.BIGINT:
				preparedStatement.setLong(index + 1, ObjectUtil.getLong(value, 0L));
				break;
			case Types.REAL:
			case Types.FLOAT:
				preparedStatement.setFloat(index + 1, ObjectUtil.getFloat(value, 0.0f));
				break;
			case Types.DOUBLE:
				preparedStatement.setDouble(index + 1, ObjectUtil.getDouble(value, 0.0));
				break;
			case Types.DECIMAL:
			case Types.NUMERIC:
				preparedStatement.setBigDecimal(index + 1, ObjectUtil.getBigDecimal(value));
				break;
			case Types.DATE:
				preparedStatement.setDate(index + 1, ObjectUtil.getDate(value));
				break;
			case Types.TIME:
				preparedStatement.setTime(index + 1, (Time) value);
				break;
			case Types.TIMESTAMP:
				preparedStatement.setTimestamp(index + 1, ObjectUtil.getTimestamp(value, new Timestamp(0)));
				break;
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				preparedStatement.setBytes(index + 1, (byte[]) value);
				break;
			default:
				preparedStatement.setObject(index + 1, value);
				LOG.warn(String.format("unknown sql type (%s) for column %s. Best effort approach to set its value: %s.", type, index + 1, value));
		}
	}
}
