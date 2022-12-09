package com.bilibili.bsql.mysql;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className ConcatArray.java
 * @description This is the description of ConcatArray.java
 * @createTime 2020-11-30 12:38:00
 */
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class ConcatArray extends ScalarFunction {

    public String eval(@DataTypeHint("STRING") String sep, @DataTypeHint("MULTISET") Map<Object, Object> map) throws IOException {
        try {
            return map.keySet().stream().map(Object::toString).collect(Collectors.joining(sep));
        } catch (Exception e) {
            return "error";
        }
    }
}
