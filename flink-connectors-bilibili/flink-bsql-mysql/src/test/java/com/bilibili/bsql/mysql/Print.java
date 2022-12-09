package com.bilibili.bsql.mysql;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author zhangyang
 * @Date:2019/11/5
 * @Time:4:08 PM
 */
public class Print extends ScalarFunction {

    private final static Logger LOG = LoggerFactory.getLogger(ScalarFunction.class);

    public Integer eval(String name, Integer value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }

    public Long eval(String name, Long value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }

    public String eval(String name, String value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }

    public Timestamp eval(String name, Timestamp value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }

    public Double eval(String name, Double value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }

    public Boolean eval(String name, Boolean value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }

    public Date eval(String name, Date value) {
        LOG.info("name:{},value:{}", name, value);
        return value;
    }
}
