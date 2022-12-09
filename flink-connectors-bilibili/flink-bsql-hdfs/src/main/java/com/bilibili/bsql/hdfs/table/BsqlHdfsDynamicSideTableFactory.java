package com.bilibili.bsql.hdfs.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.hdfs.tableinfo.HdfsSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.hdfs.tableinfo.HdfsConfig.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHdfsDynamicTableFactory.java
 * @description This is the description of BsqlHdfsDynamicTableFactory.java
 * @createTime 2020-10-22 21:55:00
 */
public class BsqlHdfsDynamicSideTableFactory extends BsqlDynamicTableSideFactory<HdfsSideTableInfo> {
    public static final String IDENTIFIER = "bsql-hdfs";

    @Override
    public HdfsSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new HdfsSideTableInfo(helper, context);
    }

    @Override
    public DynamicTableSource generateTableSource(HdfsSideTableInfo sourceTableInfo) {
        return new BsqlHdfsDynamicSideSource(sourceTableInfo);
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_HDFS_PATH);
		option.add(BSQL_HDFS_DELIMITER_KEY);
		option.add(BSQL_HDFS_PERIOD);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> option) {

	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
