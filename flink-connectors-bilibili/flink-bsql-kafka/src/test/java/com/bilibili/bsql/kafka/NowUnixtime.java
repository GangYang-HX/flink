/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka;

import java.sql.Timestamp;
import java.util.Optional;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 *
 * @author zhouxiaogang
 * @version $Id: NowUnixtime.java, v 0.1 2020-11-02 16:49
zhouxiaogang Exp $$
 */
public class NowUnixtime extends ScalarFunction {
    public Long eval() {
        return System.currentTimeMillis();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()

                .outputTypeStrategy(callContext -> {

                    // return a data type based on a literal
                            return Optional.of(DataTypes.BIGINT().notNull());

                })
                .build();
    }
}