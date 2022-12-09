/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;

/**
 *
 * @author zhouxiaogang
 * @version $Id: ToTimestampSafe.java, v 0.1 2020-12-14 14:41
zhouxiaogang Exp $$
 */
public class ToTimestampSafe extends ScalarFunction {

    public Timestamp eval(Long unixTimestamp) {
        return new Timestamp(unixTimestamp);
    }

    public Timestamp eval(String unixTimestamp) {
        if (StringUtils.isEmpty(unixTimestamp)) {
            return new Timestamp(0);
        }

        try {
            return new Timestamp(Long.parseLong(unixTimestamp.trim()));
        } catch (Exception e) {
            // ignore
        }
        return new Timestamp(0);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP;
    }
}
