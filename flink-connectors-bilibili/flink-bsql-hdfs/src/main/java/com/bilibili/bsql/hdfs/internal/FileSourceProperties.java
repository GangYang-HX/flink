package com.bilibili.bsql.hdfs.internal;

import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;

/**
 * @author zhangyang
 * @Date:2020/5/18
 * @Time:3:41 PM
 */
public class FileSourceProperties implements Serializable {

    private String path;
    private Integer fetchSize;
    private String confDir;
    private String userName;
    private String delimiter;
    private RowTypeInfo schema;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public RowTypeInfo getSchema() {
        return schema;
    }

    public void setSchema(RowTypeInfo schema) {
        this.schema = schema;
    }

    public String getConfDir() {
        return confDir;
    }

    public void setConfDir(String confDir) {
        this.confDir = confDir;
    }
}
