package com.bilibili.bsql.rdb.tableinfo;

import com.bilibili.bsql.common.SinkTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;

import static com.bilibili.bsql.rdb.tableinfo.RdbConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RdbSinkTableInfo.java
 * @description This is the description of RdbSinkTableInfo.java
 * @createTime 2020-10-20 15:49:00
 */
@Data
public class RdbSinkTableInfo extends SinkTableInfo implements Serializable {

	private String dbURL;
	private String userName;
	private String password;
	private int[] sqlTypes;
	private String tableName;
	private String tps;
	private Integer insertType;
	private String realTableNames;
	private String targetDBs;
	private String targetTables;
	private String autoCommit;
	private Integer maxRetries;

	// TODO Are the parameters useful
	public RdbSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.dbURL = helper.getOptions().get(BSQL_URL);
		this.userName = helper.getOptions().get(BSQL_UNAME);
		this.password = helper.getOptions().get(BSQL_PSWD);
		this.tableName = helper.getOptions().get(BSQL_TABLE_NAME);
		this.tps = helper.getOptions().get(BSQL_TPS);
		this.insertType = helper.getOptions().get(BSQL_INSERT_TYPE);
		this.realTableNames = helper.getOptions().get(BSQL_REAL_TABLE_NAMES);
		this.targetDBs = helper.getOptions().get(BSQL_TARGET_DBS);
		this.targetTables = helper.getOptions().get(BSQL_TARGET_TABLES);
		this.autoCommit = helper.getOptions().get(BSQL_AUTO_COMMIT);
		this.maxRetries = helper.getOptions().get(BSQL_MYSQL_MAX_RETRIES);
		checkArgument(StringUtils.isNotEmpty(dbURL), String.format("sink表:%s 没有设置url属性", getName()));
		checkArgument(StringUtils.isNotEmpty(tableName), String.format("sink表:%s 没有设置tableName属性", getName()));
		checkArgument(StringUtils.isNotEmpty(userName), String.format("sink表:%s 没有设置userName属性", getName()));
		checkArgument(StringUtils.isNotEmpty(password), String.format("sink表:%s 没有设置password属性", getName()));
	}

	@Override
	public String getType() {
		return super.getType().toLowerCase();
	}
}
