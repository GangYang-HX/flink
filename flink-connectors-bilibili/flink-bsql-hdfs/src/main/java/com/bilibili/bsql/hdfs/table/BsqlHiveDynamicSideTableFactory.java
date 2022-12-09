package com.bilibili.bsql.hdfs.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import javax.crypto.spec.OAEPParameterSpec;
import java.util.Set;

import static com.bilibili.bsql.hdfs.tableinfo.HiveConfig.*;

/**
 * @Author: JinZhengyu
 * @Date: 2022/7/28 上午10:09
 */
public class BsqlHiveDynamicSideTableFactory extends BsqlDynamicTableSideFactory<HiveSideTableInfo> {
    public static final String IDENTIFIER = "bsql-hive-side";

    @Override
    public HiveSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new HiveSideTableInfo(helper, context);
    }

    @Override
    public DynamicTableSource generateTableSource(HiveSideTableInfo sourceTableInfo) {
        return new BsqlHiveDynamicSideSource(sourceTableInfo);
    }

    @Override
    public void setRequiredOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_HIVE_TABLE_NAME);
        option.add(BSQL_HIVE_JOIN_KEY);
    }

    @Override
    public void setOptionalOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_HIVE_CHECK_SUCCESS);
        option.add(BSQL_USE_MEM);
        option.add(BSQL_JOIN_LOG);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
