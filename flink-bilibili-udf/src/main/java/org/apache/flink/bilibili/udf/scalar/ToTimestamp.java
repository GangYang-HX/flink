package org.apache.flink.bilibili.udf.scalar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author zhangyang
 * @Date:2019/11/4
 * @Time:6:21 PM
 */
public class ToTimestamp extends ScalarFunction {

    public Timestamp eval(Long unixTimestamp) {
        return new Timestamp(unixTimestamp);
    }

    public Timestamp eval(String unixTimestamp) {
        if (StringUtils.isEmpty(unixTimestamp)) {
            return null;
        }

        try {
            return new Timestamp(Long.parseLong(unixTimestamp.trim()));
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP;
    }
}
