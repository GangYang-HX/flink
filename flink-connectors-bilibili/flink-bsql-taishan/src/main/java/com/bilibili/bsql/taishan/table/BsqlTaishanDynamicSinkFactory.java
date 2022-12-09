package com.bilibili.bsql.taishan.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.taishan.tableinfo.TaishanSinkTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;


import java.util.Set;

import static com.bilibili.bsql.taishan.tableinfo.TaishanConfig.*;
/**
 * @author zhuzhengjun
 * @date 2020/11/2 12:14 下午
 */
public class BsqlTaishanDynamicSinkFactory extends BsqlDynamicTableSinkFactory<TaishanSinkTableInfo> {

    public static final String IDENTIFIER = "bsql-taishan";

    @Override
    public TaishanSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new TaishanSinkTableInfo(helper, context);
    }

    @Override
    public DynamicTableSink generateTableSink(TaishanSinkTableInfo sinkTableInfo) {
        return new BsqlTaishanDynamicSink(sinkTableInfo);
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
        option.add(BSQL_TAISHAN_TTL);

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
