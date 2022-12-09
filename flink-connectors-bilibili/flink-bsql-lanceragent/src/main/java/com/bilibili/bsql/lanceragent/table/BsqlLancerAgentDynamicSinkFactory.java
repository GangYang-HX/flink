package com.bilibili.bsql.lanceragent.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.lanceragent.tableinfo.LancerAgentSinkTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.common.format.BsqlDelimitConfig.DELIMITER_KEY;
import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_PARALLELISM;
import static com.bilibili.bsql.lanceragent.tableinfo.LancerAgentConfig.BSQL_LANCER_AGENT_DELIMITER;
import static com.bilibili.bsql.lanceragent.tableinfo.LancerAgentConfig.BSQL_LANCER_AGENT_LOG_ID;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlLancerAgentDynamicSinkFactory.java
 * @description This is the description of BsqlLancerAgentDynamicSinkFactory.java
 * @createTime 2020-11-10 18:30:00
 */
public class BsqlLancerAgentDynamicSinkFactory extends BsqlDynamicTableSinkFactory<LancerAgentSinkTableInfo> {
    public static final String IDENTIFIER = "bsql-lanceragent";

    @Override
    public LancerAgentSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new LancerAgentSinkTableInfo(helper, context);
    }

    @Override
    public DynamicTableSink generateTableSink(LancerAgentSinkTableInfo sinkTableInfo) {
        return new BsqlLancerAgentDynamicSink(sinkTableInfo);
    }

    @Override
    public void setRequiredOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_LANCER_AGENT_DELIMITER);
        option.add(BSQL_LANCER_AGENT_LOG_ID);
        option.add(BSQL_PARALLELISM);
    }

    @Override
    public void setOptionalOptions(Set<ConfigOption<?>> option) {

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
