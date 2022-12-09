package com.bilibili.bsql.vbase.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.vbase.tableinfo.VbaseSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.vbase.tableinfo.VbaseConfig.*;
import java.util.Set;

/**
 * @author zhuzhengjun
 * @date 2020/11/4 11:14 上午
 */
public class BsqlVbaseDynamicSideTableFactory extends BsqlDynamicTableSideFactory<VbaseSideTableInfo> {

    public static final String IDENTIFIER = "bsql-vbase";

    @Override
    public VbaseSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new VbaseSideTableInfo(helper, context);
    }

    @Override
    public DynamicTableSource generateTableSource(VbaseSideTableInfo sourceTableInfo) {
        return new BsqlVbaseDynamicSideSource(sourceTableInfo);
    }

    @Override
    public void setRequiredOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_VBASE_HOST);
        option.add(BSQL_VBASE_PARENT);
        option.add(BSQL_VBASE_TABLENAME);
    }

    @Override
    public void setOptionalOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_VBASE_PORT);
        option.add(BSQL_VBASE_ROWKEYNAME);
        option.add(BSQL_VBASE_TIMEOUTMS);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

}
