package com.bilibili.bsql.mysql;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className NowTs.java
 * @description This is the description of NowTs.java
 * @createTime 2020-11-03 17:51:00
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
