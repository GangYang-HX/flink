package com.bilibili.bsql.taishan.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.taishan.tableinfo.TaishanSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.taishan.tableinfo.TaishanConfig.*;
import static com.bilibili.bsql.taishan.tableinfo.TaishanConfig.BSQL_TAISHAN_PASSWORD;

public class BsqlTaishanDynamicSideFactory extends BsqlDynamicTableSideFactory<TaishanSideTableInfo> {

    public static final String IDENTIFIER = "bsql-taishan";

    @Override
    public TaishanSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new TaishanSideTableInfo(helper, context);
    }

    @Override
    public DynamicTableSource generateTableSource(TaishanSideTableInfo sourceTableInfo) {
        return new BsqlTaishanLookupTableSource(sourceTableInfo);
    }

    @Override
    public void setRequiredOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_TAISHAN_TABLE);
        option.add(BSQL_TAISHAN_ZONE);
        option.add(BSQL_TAISHAN_CLUSTER);
        option.add(BSQL_TAISHAN_PASSWORD);
    }

    @Override
    public void setOptionalOptions(Set<ConfigOption<?>> option) {

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
