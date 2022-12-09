package com.bilibili.bsql.common.api.sink;

/**
 * Retry sender interface.
 *
 * @param <OUT>
 */
public interface RetrySender<OUT> {

    void sendRecord(OUT record);

    void close();
}
