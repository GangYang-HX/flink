package com.bilibili.bsql.hdfs.internal;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhangyang
 * @Date:2020/5/19
 * @Time:4:17 PM
 */
public class FilePartition implements Serializable {

    private List<String> fileList;
    private int fileIndex = 0;
    private long fileOffset = 0;

    public FilePartition(List<String> fileList) {
        this.fileList = fileList;
    }

    public List<String> getFileList() {
        return fileList;
    }

    public void setFileList(List<String> fileList) {
        this.fileList = fileList;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public void setFileIndex(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
    }
}
