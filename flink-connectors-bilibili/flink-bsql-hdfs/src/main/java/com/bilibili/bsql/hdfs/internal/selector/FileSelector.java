package com.bilibili.bsql.hdfs.internal.selector;


import com.bilibili.bsql.hdfs.internal.FilePartition;
import com.bilibili.bsql.hdfs.internal.FileSourceProperties;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhangyang
 * @Date:2020/5/19
 * @Time:6:19 PM
 */
public interface FileSelector extends Serializable {

    /**
     * selectAndSort
     *
     * @param index
     * @param num
     * @param properties
     * @return
     * @throws Exception
     */
    List<FilePartition> selectAndSort(int index, int num, FileSourceProperties properties) throws Exception;
}
