package com.bilibili.bsql.clickhouse.table;

import com.bilibili.bsql.clickhouse.format.ClickHouseOutputFormat;
import com.bilibili.bsql.rdb.format.RetractJDBCOutputFormat;
import com.bilibili.bsql.rdb.table.BsqlRdbDynamicSinkBase;
import com.bilibili.bsql.rdb.tableinfo.RdbSinkTableInfo;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlClickHouseDynamicSink.java
 * @description This is the description of BsqlClickHouseDynamicSink.java
 * @createTime 2020-10-21 14:43:00
 */
public class BsqlClickHouseDynamicSink extends BsqlRdbDynamicSinkBase {

	private static final String CLICK_HOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
	private final DataType dataType;
	private final RdbSinkTableInfo sinkTableInfo;

	public BsqlClickHouseDynamicSink(RdbSinkTableInfo sinkTableInfo) {
		super(sinkTableInfo);
		this.sinkTableInfo = sinkTableInfo;
		this.dataType = sinkTableInfo.getPhysicalRowDataType();
	}

	@Override
	public void buildSql(String tableName, List<String> fields, List<String> primaryKeys) {
		buildInsertSql(tableName, fields);
	}

	private void buildInsertSql(String tableName, List<String> fields) {
		String sqlTmp = "INSERT INTO " + tableName + " (${fields}) values (${placeholder})";
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

	@Override
	public String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
		throw new UnsupportedOperationException("not support now");
	}

	@Override
	public RetractJDBCOutputFormat getOutputFormat(Context context) {
		return new ClickHouseOutputFormat(dataType, context);
	}

	@Override
	public String getDriverName() {
		return CLICK_HOUSE_DRIVER;
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlClickHouseDynamicSink(sinkTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "ClickHouse-Sink-" + sinkTableInfo.getName();
	}
}
