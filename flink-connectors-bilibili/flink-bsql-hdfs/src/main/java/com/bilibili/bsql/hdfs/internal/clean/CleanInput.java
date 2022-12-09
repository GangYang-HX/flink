package com.bilibili.bsql.hdfs.internal.clean;

import java.io.Serializable;

/**
 * @author zhangyang
 * @Date:2020/7/23
 * @Time:3:01 PM
 */
public class CleanInput implements Serializable {

    private int index;
    private int num;
    private boolean isEnd;

    public CleanInput(int index, int num, boolean isEnd) {
        this.index = index;
        this.num = num;
        this.isEnd = isEnd;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
