package com.bilibili.bsql.mysql.cache;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class NoCompressionCache implements Cache {

    private static final Logger LOG = LoggerFactory.getLogger(NoCompressionCache.class);

    private final List<RowData> records = new ArrayList<>();
    private final int subtaskId;

    public NoCompressionCache(int subtaskId) {
        this.subtaskId = subtaskId;
    }

    @Override
    public void addData(RowData rowData) {
        // 撤回操作忽略
        // RowKind.UPDATE_AFTER means retract
        if (rowData.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
            return;
        }
        records.add(rowData);
    }

    @Override
    public void clearData() {
        records.clear();
    }

    @Override
    public void consumeData(Consumer<RowData> consumer) {
        LOG.info("subtaskId: {}, write records count:{}", subtaskId, records.size());
        records.forEach(consumer);
    }

    @Override
    public boolean isEmpty() {
        return records.size() == 0;
    }


}
