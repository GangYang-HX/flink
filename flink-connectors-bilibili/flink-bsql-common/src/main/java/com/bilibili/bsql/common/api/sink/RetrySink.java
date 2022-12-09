package com.bilibili.bsql.common.api.sink;

import java.io.Serializable;

/**
 * retry sink interface.
 *
 * @param <OUT>
 */
public interface RetrySink<OUT> extends Serializable {

    void start() throws Exception;

    void addRetryRecord(OUT record) throws Exception;

    void close() throws Exception;

    Integer getPendingRecordsSize();
}
