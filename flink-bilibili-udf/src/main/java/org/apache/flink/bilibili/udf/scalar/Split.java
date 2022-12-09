/**
 * Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved.
 */

package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang.StringUtils;

/**
 * @author zhouxiaogang
 * @date 2020-02-24 17:32
 */
public class Split extends ScalarFunction {

    public String[] eval(String value, String delimiter) {
        if (StringUtils.isEmpty(value)) {
            return new String[0];
        }
        return StringUtils.splitByWholeSeparatorPreserveAllTokens(value, delimiter);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return ObjectArrayTypeInfo.getInfoFor(Types.STRING);
    }

}
