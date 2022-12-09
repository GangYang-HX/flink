package com.bilibili.bsql.hdfs.internal.clean;

import java.io.Serializable;

/**
 * @author zhangyang
 * @Date:2020/7/23
 * @Time:3:06 PM
 */
public class CleanOutput implements Serializable {

    private boolean allEnd;

    public CleanOutput(boolean allEnd) {
        this.allEnd = allEnd;
    }

    public boolean isAllEnd() {
        return allEnd;
    }

    public void setAllEnd(boolean allEnd) {
        this.allEnd = allEnd;
    }
}
