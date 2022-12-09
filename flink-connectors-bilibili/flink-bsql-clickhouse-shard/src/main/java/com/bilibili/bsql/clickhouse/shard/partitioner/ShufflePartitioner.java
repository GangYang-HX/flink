
package com.bilibili.bsql.clickhouse.shard.partitioner;

import com.google.common.collect.Multimap;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.util.concurrent.ThreadLocalRandom;

public class ShufflePartitioner extends BalancedPartitioner {
    private static final long serialVersionUID = 1L;

    public ShufflePartitioner(Multimap<Integer, ClickHouseConnection> shardConnections, int batchSize, int subtaskId) {
        super(shardConnections, batchSize, subtaskId);
    }

    @Override
    public int select(final RowData record) {
        return ThreadLocalRandom.current().nextInt(shardExecutors.size());
    }
}
