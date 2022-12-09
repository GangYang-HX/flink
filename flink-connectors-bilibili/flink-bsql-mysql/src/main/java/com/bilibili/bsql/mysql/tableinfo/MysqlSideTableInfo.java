package com.bilibili.bsql.mysql.tableinfo;

import com.bilibili.bsql.rdb.tableinfo.RdbSideTableInfo;
import lombok.Data;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.mysql.tableinfo.MysqlConfig.BSQL_IDLE_CONNECTION_TEST_PERIOD_TYPE;
import static com.bilibili.bsql.mysql.tableinfo.MysqlConfig.BSQL_MAX_IDLE_TIME;
import static com.bilibili.bsql.mysql.tableinfo.MysqlConfig.BSQL_TM_CONNECTION_POOL_SIZE;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className MysqlSideTableInfo.java
 * @description This is the description of MysqlSideTableInfo.java
 * @createTime 2020-10-26 16:51:00
 */
@Data
public class MysqlSideTableInfo extends RdbSideTableInfo {
	public static final String CURR_TYPE = "mysql";
	private Integer idleConnectionTestPeriod;
	private Integer maxIdleTime;
	private Integer tmConnectPoolSize;

	public MysqlSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		setType(CURR_TYPE);
		this.idleConnectionTestPeriod = helper.getOptions().get(BSQL_IDLE_CONNECTION_TEST_PERIOD_TYPE);
		this.maxIdleTime = helper.getOptions().get(BSQL_MAX_IDLE_TIME);
		this.tmConnectPoolSize = helper.getOptions().get(BSQL_TM_CONNECTION_POOL_SIZE);
	}
}
