package com.bilibili.bsql.mysql.table;

import com.bilibili.bsql.mysql.format.MysqlDeletableOutputFormatWrapper;
import com.bilibili.bsql.mysql.format.MysqlOutputFormat;
import com.bilibili.bsql.mysql.sharding.ShardingMysqlOutputFormat;
import com.bilibili.bsql.mysql.tableinfo.MysqlSinkTableInfo;
import com.bilibili.bsql.rdb.format.RetractJDBCOutputFormat;
import com.bilibili.bsql.rdb.table.BsqlRdbDynamicSinkBase;
import com.bilibili.bsql.rdb.tableinfo.RdbSinkTableInfo;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlMysqlDynamicSink.java
 * @description This is the description of BsqlMysqlDynamicSink.java
 * @createTime 2020-10-22 15:04:00
 */
public class BsqlMysqlDynamicSink extends BsqlRdbDynamicSinkBase {
	private static final Logger LOG = LoggerFactory.getLogger(BsqlMysqlDynamicSink.class);
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private final MysqlSinkTableInfo sinkTableInfo;
	private final DataType dataType;

	public BsqlMysqlDynamicSink(MysqlSinkTableInfo sinkTableInfo) {
		super(sinkTableInfo);
		this.sinkTableInfo = sinkTableInfo;
		this.dataType = sinkTableInfo.getPhysicalRowDataType();
	}

	@Override
	public void buildSql(String tableName, List<String> fields, List<String> primaryKeys) {
		buildInsertSql(tableName, fields, primaryKeys);
	}

	@Override
	public String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
		throw new UnsupportedOperationException("not support now");
	}

	@Override
	public RetractJDBCOutputFormat getOutputFormat(Context context) {
		if (null != realTableNames && null != targetDBs && null != targetTables) {
			return new ShardingMysqlOutputFormat(dataType, context);
		} else {
		    MysqlOutputFormat mysqlOutputFormat = new MysqlOutputFormat(dataType, context);
		    mysqlOutputFormat.setEnableCompression(sinkTableInfo.isEnableCompress());
			return mysqlOutputFormat;
		}
	}

	@Override
	public SinkFunction<RowData> createSinkFunction(
		RdbSinkTableInfo sinkTableInfo, Context context) {
		SinkFunction<RowData> sinkFunction = super.createSinkFunction(sinkTableInfo, context);
		if (this.sinkTableInfo.isEnableDelete()) {
			sinkFunction =
				new MysqlDeletableOutputFormatWrapper(
					context, this.sinkTableInfo, (MysqlOutputFormat) sinkFunction)
					.getJdbcBatchingSinkFunction();
			LOG.info(String.format("converted to %s for delete", sinkFunction.getClass().getName()));
		}

		return sinkFunction;
	}

	@Override
	public String getDriverName() {
		return MYSQL_DRIVER;
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlMysqlDynamicSink(sinkTableInfo);
	}

	private void buildInsertSql(String tableName, List<String> fields, List<String> primaryKeys) {
		if (insertType == 0) {
			String sqlTmp = "INSERT INTO " + tableName + " (${fields}) values (${placeholder})" + " ON DUPLICATE KEY UPDATE ${update}";
			String fieldsStr = "";
			String placeholder = "";
			String update = "";
			for (String fieldName : fields) {
				fieldsStr += ",`" + fieldName + "`";
				placeholder += ",?";
				if (primaryKeys == null) {
					throw new RuntimeException("必须给mysql sink表设置主键, 请增加主键.");
				}
				if (!primaryKeys.contains(fieldName)) {
					update += ",`" + fieldName + "`= ?";
				}
			}
			fieldsStr = fieldsStr.replaceFirst(",", "");
			placeholder = placeholder.replaceFirst(",", "");
			update = update.replaceFirst(",", "");
			sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder).replace("${update}", update);
			this.sql = sqlTmp;
		} else {
			String sqlTmp = "INSERT IGNORE INTO " + tableName + " (${fields}) values (${placeholder})";
			String fieldsStr = "";
			String placeholder = "";
			for (String fieldName : fields) {
				fieldsStr += ",`" + fieldName + "`";
				placeholder += ",?";
			}
			fieldsStr = fieldsStr.replaceFirst(",", "");
			placeholder = placeholder.replaceFirst(",", "");
			sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
			this.sql = sqlTmp;
		}
		LOG.info("sql:{}", this.sql);
	}
}
