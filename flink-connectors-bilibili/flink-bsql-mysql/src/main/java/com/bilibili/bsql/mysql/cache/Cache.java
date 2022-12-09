package com.bilibili.bsql.mysql.cache;

import org.apache.flink.table.data.RowData;

import java.util.function.Consumer;

public interface Cache {

    void addData(RowData rowData);

    void clearData();

    void consumeData(Consumer<RowData> consumer);

    boolean isEmpty();

    default void validPrimaryKey() {
        //do nothing
    }
}
