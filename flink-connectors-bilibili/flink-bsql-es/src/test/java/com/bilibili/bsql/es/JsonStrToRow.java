package com.bilibili.bsql.es;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className JsonToMap.java
 * @description This is the description of JsonToMap.java
 * @createTime 2020-12-10 14:03:00
 */
public class JsonStrToRow extends ScalarFunction {

    @DataTypeHint("ROW<s STRING>")
    public Row eval(String json) {
        try {
            return Row.of(json);
        } catch (Exception e) {
            return null;
        }
    }
}
