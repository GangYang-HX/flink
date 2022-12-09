package com.bilibili.bsql.clickhouse.shard.partitioner;

import com.bilibili.bsql.clickhouse.shard.executor.ClickHouseExecutor;
import com.google.common.collect.Multimap;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BalancedPartitioner extends AbstractPartitioner {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BalancedPartitioner.class);
    private static final int SINGLE_REPLICA_FLUSH_MAX_TIMES = 3;
    private int flushTimes;
    private final int batchSize;
    private int nextShard;
    private int batchFlushIndex;

    protected final List<ClickHouseExecutor> shardExecutors;

    public BalancedPartitioner(Multimap<Integer, ClickHouseConnection> shardConnections, int batchSize, int subtaskId) {
        super(subtaskId, shardConnections);
        this.batchSize = batchSize;
        this.nextShard = 0;
        this.shardExecutors = new ArrayList<>();
        this.batchFlushIndex = subtaskId % shardConnections.values().size();
        LOG.info("subtaskId: {} set first flush index: {}", subtaskId, batchFlushIndex);
    }

    @Override
    public int select(final RowData record) {
        return this.nextShard = (this.nextShard + 1) % shardExecutors.size();
    }

    @Override
    public void addShardExecutors(int shardNum, ClickHouseExecutor clickHouseExecutor) {
        shardExecutors.add(clickHouseExecutor);
    }

    @Override
    public int getShardNum() {
        return shardExecutors.size();
    }

    @Override
    public void write(List<RowData> records, int shardIndex, int replicaIndex) throws IOException {
        shardExecutors.get(shardIndex).addBatch(records);
    }

    @Override
    public void flush(int shardIndex, int replicaIndex) throws IOException {
        shardExecutors.get(replicaIndex).executeBatch();
    }

    @Override
    public void batchFlush(List<RowData> data) throws IOException {
        if (data.isEmpty()) {
            return;
        }
        for (int retry = 1; ; retry++) {
            try {
                shardExecutors.get(batchFlushIndex).addBatch(data);
                shardExecutors.get(batchFlushIndex).executeBatch();
                LOG.info("subtask {} batch flush {} records to {} success!", subtaskId, data.size(), batchFlushIndex);
                batchFlushIndex = getNextIndex(data.size(), false);
                break;
            } catch (Exception e) {
                if (retry == shardExecutors.size()) {
                    throw new IOException("All nodes are unavailable, can not flush!", e);
                }
                LOG.info("subtask {} batch flush to {} error, retry times: {}, will try flush again!", subtaskId,
                        batchFlushIndex, retry, e);
                batchFlushIndex = getNextIndex(data.size(), true);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        for (ClickHouseExecutor shardExecutor : this.shardExecutors) {
            shardExecutor.closeStatement();
        }
    }

    private int getNextIndex(int count, boolean mandatory) {
        if (mandatory) {
            flushTimes = 0;
            int tmpIndex = batchFlushIndex + 1;
            return tmpIndex == shardExecutors.size() ? 0 : tmpIndex;
        } else {
            if (flushTimes > SINGLE_REPLICA_FLUSH_MAX_TIMES || count > batchSize / 2) {
                return getNextIndex(count, true);
            }
            LOG.info("subtask {} will not change index, because flushTimes={}, count={}", subtaskId, flushTimes, count);
            flushTimes++;
            return batchFlushIndex;
        }

    }

}
