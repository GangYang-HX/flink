package com.bilibili.bsql.clickhouse.shard.executor;

import com.bilibili.bsql.clickhouse.shard.converter.ClickHouseRowConverter;
import com.bilibili.bsql.common.metrics.SinkMetricsWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseBatchExecutor implements ClickHouseExecutor {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);
	private transient ClickHousePreparedStatement stmt;
	private final String sql;
	private final ClickHouseRowConverter converter;

	private final int maxRetries;
	protected final int[] sqlTypes;
	private final SinkMetricsWrapper sinkMetricsGroup;
	private final int batchMaxCount;


	public ClickHouseBatchExecutor(final String sql, final ClickHouseRowConverter converter, final int batchMaxCount, final int maxRetries, final int[] sqlTypes, SinkMetricsWrapper sinkMetricsWrapper) {

		this.sql = sql;
		this.converter = converter;
		this.batchMaxCount = batchMaxCount;
		this.maxRetries = maxRetries;
		this.sqlTypes = sqlTypes;
		this.sinkMetricsGroup = sinkMetricsWrapper;

	}

	@Override
	public void prepareStatement(final ClickHouseConnection connection) throws SQLException {
		this.stmt = (ClickHousePreparedStatement) connection.prepareStatement(this.sql);
	}

	@Override
	public void addBatch(List<RowData> records) throws IOException {
		try {
			for (final RowData r : records) {
				if (r.getRowKind() != RowKind.DELETE && r.getRowKind() != RowKind.UPDATE_BEFORE) {
					this.converter.insertOrIgnore(r, this.stmt);
					this.stmt.addBatch();
				}
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void executeBatch() throws IOException {
		try {
			this.stmt.executeBatch();
		} catch (SQLException e) {
			throw new IOException(e);
		} finally {
			try {
				this.stmt.clearBatch();
			} catch (SQLException ex) {
				throw new IOException(ex);
			}
		}
	}

	@Override
	public void closeStatement() throws SQLException {
		if (this.stmt != null) {
			this.stmt.close();
			this.stmt = null;
		}
	}
}
