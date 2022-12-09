package com.bilibili.bsql.hbase.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.hbase.tableinfo.HbaseSideTableInfo;
import com.bilibili.bsql.hbase.util.HBaseTableSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;
import static com.bilibili.bsql.hbase.tableinfo.HbaseSideTableInfo.*;


/**
 * @author zhuzhengjun
 * @date 2020/11/12 6:55 下午
 */
public class BsqlHbaseDynamicSideTableFactory extends BsqlDynamicTableSideFactory<HbaseSideTableInfo> {


    public static final String IDENTIFIER = "bsql-hbase";


    @Override
    public HbaseSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new HbaseSideTableInfo(helper, context);
    }

    @Override
    public DynamicTableSource generateTableSource(HbaseSideTableInfo sourceTableInfo) {
        return new BsqlHbaseDynamicSideSource(sourceTableInfo);
    }

    @Override
    public void setRequiredOptions(Set<ConfigOption<?>> option) {
        option.add(TABLE_NAME);
        option.add(ZOOKEEPER_QUORUM);
        option.add(ZOOKEEPER_ZNODE_PARENT);
    }

    @Override
    public void setOptionalOptions(Set<ConfigOption<?>> option) {
        option.add(HBASE_NULL_STRING_LITERAL);

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
