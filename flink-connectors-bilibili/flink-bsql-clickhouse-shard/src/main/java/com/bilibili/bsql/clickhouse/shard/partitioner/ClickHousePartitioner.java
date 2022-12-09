package com.bilibili.bsql.clickhouse.shard.partitioner;

import com.bilibili.bsql.clickhouse.shard.executor.ClickHouseExecutor;
import com.bilibili.bsql.common.failover.BatchOutFormatFailOver;
import com.bilibili.bsql.common.failover.FailOverConfigInfo;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;


public interface ClickHousePartitioner extends Serializable {

    int select(final RowData p0);

    void addShardExecutors(int shardNum, ClickHouseExecutor clickHouseExecutor);

    int getShardNum();

    BatchOutFormatFailOver initFailOver(FailOverConfigInfo config) throws IllegalAccessException;

    void write(List<RowData> records, int shardIndex, int replicaIndex) throws IOException;

    void flush(int shardIndex, int replicaIndex) throws IOException;

    void batchFlush(List<RowData> data) throws IllegalAccessException, IOException;

    void close() throws SQLException;
}
