package com.bilibili.bsql.clickhouse.shard.executor;

import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface ClickHouseExecutor extends Serializable
{
    void prepareStatement(final ClickHouseConnection p0) throws SQLException;

    void addBatch(final List<RowData> p0) throws IOException;
    
    void executeBatch() throws IOException;
    
    void closeStatement() throws SQLException;


}
