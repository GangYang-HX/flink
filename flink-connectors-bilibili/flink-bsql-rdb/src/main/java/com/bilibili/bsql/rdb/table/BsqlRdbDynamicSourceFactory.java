package com.bilibili.bsql.rdb.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.rdb.tableinfo.RdbSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.rdb.tableinfo.RdbConfig.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlRdbDynamicTableFactory.java
 * @description This is the description of BsqlRdbDynamicTableFactory.java
 * @createTime 2020-10-20 14:16:00
 */
public class BsqlRdbDynamicSourceFactory extends BsqlDynamicTableSideFactory<RdbSideTableInfo> {
	public static final String IDENTIFIER = "bsql-rdb";

	@Override
	public RdbSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
		return new RdbSideTableInfo(helper, context);
	}

	@Override
	public DynamicTableSource generateTableSource(RdbSideTableInfo sourceTableInfo) {
		return new BsqlRdbDynamicSource(sourceTableInfo);
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_URL);
		option.add(BSQL_PSWD);
		option.add(BSQL_UNAME);
		option.add(BSQL_TABLE_NAME);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_CONNECTION_POOL_SIZE);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
