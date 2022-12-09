package com.bilibili.bsql.lanceragent.table;

import com.bilibili.bsql.lanceragent.function.LancerAgentSinkFunction;
import com.bilibili.bsql.lanceragent.tableinfo.LancerAgentSinkTableInfo;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlLancerAgentDynamicSink.java
 * @description This is the description of BsqlLancerAgentDynamicSink.java
 * @createTime 2020-11-10 18:35:00
 */
public class BsqlLancerAgentDynamicSink implements DynamicTableSink, Serializable {

    private final LancerAgentSinkTableInfo sinkTableInfo;

    public BsqlLancerAgentDynamicSink(LancerAgentSinkTableInfo sinkTableInfo) {
        this.sinkTableInfo = sinkTableInfo;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProviderWithParallel.of(new LancerAgentSinkFunction(sinkTableInfo, context), sinkTableInfo.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new BsqlLancerAgentDynamicSink(sinkTableInfo);
    }

    @Override
    public String asSummaryString() {
        return "LancerAgent-Sink-" + sinkTableInfo.getName();
    }
}
