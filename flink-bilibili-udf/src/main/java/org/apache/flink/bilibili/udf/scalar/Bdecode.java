package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author zhangyang
 * @Date:2019/11/11
 * @Time:4:03 PM
 */
public class Bdecode extends ScalarFunction {

    private UrlDecode urlDecode;

    @Override
    public void open(FunctionContext context) {
        urlDecode = new UrlDecode();
    }

    public String eval(String str) {
        return urlDecode.eval(str);
    }
}
