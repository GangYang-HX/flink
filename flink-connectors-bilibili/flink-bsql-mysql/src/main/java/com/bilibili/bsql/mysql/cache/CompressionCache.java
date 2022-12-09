package com.bilibili.bsql.mysql.cache;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class CompressionCache implements Cache {

    private static final Logger LOG = LoggerFactory.getLogger(CompressionCache.class);

    private final Map<String, RowData> primaryKeyDistinctMap = new HashMap<>();
    private final int subtaskId;
    private final List<Integer> primaryKeysIndex;
    private final Integer batchMaxCount;
    private final DynamicTableSink.DataStructureConverter converter;
    private final int insertType;

    public CompressionCache(int subtaskId, List<Integer> primaryKeysIndex, Integer batchMaxCount,
                            DynamicTableSink.DataStructureConverter converter, int insertType) {
        this.subtaskId = subtaskId;
        this.primaryKeysIndex = primaryKeysIndex;
        this.batchMaxCount = batchMaxCount;
        this.converter = converter;
        this.insertType = insertType;
    }

    @Override
    public void addData(RowData rowData) {
        // 撤回操作忽略
        // RowKind.UPDATE_AFTER means retract
        if (rowData.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
            return;
        }
        Row row = (Row) converter.toExternal(rowData);
        StringBuilder primaryKey = new StringBuilder();
        for (Integer keysIndex : primaryKeysIndex) {
            primaryKey.append(row.getField(keysIndex)).append("SaberSys");
        }
        if (insertType == 0) {
            //仅保存最新值
            primaryKeyDistinctMap.put(primaryKey.toString(), rowData);
        } else if (insertType == 1) {
            //仅保存最早值
            primaryKeyDistinctMap.putIfAbsent(primaryKey.toString(), rowData);
        }
    }

    @Override
    public void clearData() {
        primaryKeyDistinctMap.clear();
    }

    @Override
    public void consumeData(Consumer<RowData> consumer) {
        LOG.info("subtaskId: {}, distinct primary key records count: {}", subtaskId, primaryKeyDistinctMap.values().size());
        primaryKeyDistinctMap.forEach((key, value) -> consumer.accept(value));
    }

    @Override
    public boolean isEmpty() {
        return MapUtils.isEmpty(primaryKeyDistinctMap);
    }

    @Override
    public void validPrimaryKey() {
        if (primaryKeysIndex.size() == 0 && batchMaxCount > 1) {
            throw new IllegalArgumentException("Enable compression must specify the primary key!(if batchMaxCount > 1)");
        }
    }
}
