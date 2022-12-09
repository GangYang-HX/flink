package com.bilibili.bsql.hdfs.internal;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;

/**
 * @author zhangyang
 * @Date:2020/5/19
 * @Time:4:52 PM
 */
public interface Deserializer<OUT> extends Serializable {

    /**
     * deserialize
     *
     * @param element
     * @return
     */
    OUT deserialize(String element);

    /**
     * getProducedType
     *
     * @return
     */
    TypeInformation<OUT> getProducedType();
}
