package com.bilibili.bsql.hdfs.internal.clean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author zhangyang
 * @Date:2020/7/23
 * @Time:3:06 PM
 */
public class CleanAccumulator implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(CleanAccumulator.class);
    private int[] endFlag;

    public void update(CleanInput input) {
        synchronized (this) {
            if (endFlag == null) {
                endFlag = new int[input.getNum()];
            }
            if (input.isEnd()) {
                LOG.info("index:{} reach end", input.getIndex());
                endFlag[input.getIndex()] = 1;
            }
        }
    }

    public boolean getResult() {
        if (endFlag == null) {
            return false;
        }
        for (int i = 0; i < endFlag.length; i++) {
            if (endFlag[i] == 0) {
                return false;
            }
        }
        return true;
    }

    public CleanAccumulator merge(CleanAccumulator acc) {
        if (acc == null || acc.endFlag == null) {
            return this;
        }
        for (int i = 0; i < endFlag.length; i++) {
            if (acc.endFlag[i] == 1) {
                endFlag[i] = 1;
            }
        }
        return this;
    }
}
