/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RetryDataWithCount.java, v 0.1 2020-12-17 16:19
zhouxiaogang Exp $$
 */
public class RetryDataWithCount {
    public ProducerRecord<byte[], byte[]> record;
    public int failedTimes;

    public RetryDataWithCount(ProducerRecord<byte[], byte[]> record, int failedTimes) {
        this.record = record;
        this.failedTimes = failedTimes;
    }
}
