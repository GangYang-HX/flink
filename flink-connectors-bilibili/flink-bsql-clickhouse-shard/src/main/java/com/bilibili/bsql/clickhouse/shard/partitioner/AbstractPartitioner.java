package com.bilibili.bsql.clickhouse.shard.partitioner;

import com.bilibili.bsql.common.failover.BatchOutFormatFailOver;
import com.bilibili.bsql.common.failover.FailOverConfigInfo;
import com.google.common.collect.Multimap;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.util.List;

public abstract class AbstractPartitioner implements ClickHousePartitioner {

    protected final Multimap<Integer, ClickHouseConnection> shardConnections;
    protected final int subtaskId;

    public AbstractPartitioner(int subtaskId, Multimap<Integer, ClickHouseConnection> shardConnections) {
        this.subtaskId = subtaskId;
        this.shardConnections = shardConnections;
    }

    @Override
    public BatchOutFormatFailOver initFailOver(FailOverConfigInfo config) throws IllegalAccessException {
        //do noting
        throw new IllegalAccessException("unwanted operation");
    }

    @Override
    public void batchFlush(List<RowData> data) throws IllegalAccessException, IOException {
        //do noting
        throw new IllegalAccessException("unwanted operation");
    }
}
