package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author zhangyang
 * @Date:2019/11/8
 * @Time:3:46 PM
 */
public class NowTs extends ScalarFunction {

    public Timestamp eval() {
        return new Timestamp(System.currentTimeMillis());
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
