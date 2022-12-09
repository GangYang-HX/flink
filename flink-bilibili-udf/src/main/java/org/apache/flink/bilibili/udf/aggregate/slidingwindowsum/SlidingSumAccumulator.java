package org.apache.flink.bilibili.udf.aggregate.slidingwindowsum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author zhangyang
 * @Date:2020/4/21
 * @Time:3:32 PM
 */
public class SlidingSumAccumulator {

    private final static Logger LOG        = LoggerFactory.getLogger(SlidingSumAccumulator.class);
    /**
     * 最大的窗口数
     */
    private static final int    MAX_BUCKET = 16;

    /**
     * 窗口id和value间隔存储,每2个数作为一个组
     */
    private static final int    SKIP       = 2;

    /**
     * 实际数据存储的列表
     */
    private int[]               timeList   = new int[MAX_BUCKET * SKIP];

    public void add(int addNumber, Long dataTime, Integer slidingMs, Integer windowSizeMs, long curMills) {

        if (windowSizeMs / slidingMs > MAX_BUCKET || windowSizeMs % slidingMs != 0) {
            throw new RuntimeException("illegal windowSize: " + windowSizeMs + " or slidingSize: " + slidingMs);
        }
        if (addNumber == 0) {
            return;
        }
        int useBucketNum = windowSizeMs / slidingMs;
        int maxWindowId = (int) (curMills / slidingMs + 1);
        int minWindowId = maxWindowId - useBucketNum + 1;

        int bucketStartIndex = MAX_BUCKET - useBucketNum;
        int curMaxWindowId = timeList[(MAX_BUCKET - 1) * SKIP];
        int curMinWindowId = curMaxWindowId - useBucketNum + 1;

        if (curMaxWindowId < minWindowId || curMinWindowId > maxWindowId) {
            int tmpWindowId = minWindowId;
            for (int i = bucketStartIndex; i < MAX_BUCKET; i++) {
                timeList[i * SKIP] = tmpWindowId++;
                timeList[i * SKIP + 1] = 0;
            }
        } else {
            int offset = maxWindowId - curMaxWindowId;
            int endIndex = MAX_BUCKET - offset;
            if (offset > 0) {
                for (int i = bucketStartIndex; i < endIndex; i++) {
                    timeList[i * SKIP] = timeList[(i + offset) * SKIP];
                    timeList[i * SKIP + 1] = timeList[(i + offset) * SKIP + 1];
                }
                // 右边补windowId和0
                int tmpWindowId = timeList[endIndex * SKIP] + 1;
                for (int i = endIndex; i < MAX_BUCKET; i++) {
                    timeList[i * SKIP] = tmpWindowId++;
                    timeList[i * SKIP + 1] = 0;
                }
            }
        }

        long curWindowId = dataTime / slidingMs + 1;
        if (curWindowId > maxWindowId || curWindowId < minWindowId) {
            LOG.error("illegal data,maxWindowId:{},minWindowId:{},curWindowId:{}", maxWindowId, minWindowId,
                      curWindowId);
            return;
        }
        long curWindowIndex = bucketStartIndex + (curWindowId - minWindowId);
        timeList[(int) curWindowIndex * SKIP + 1] += addNumber;
    }

    public long getValue() {
        long count = 0;
        for (int i = 0; i < MAX_BUCKET; i++) {
            count += timeList[i * SKIP + 1];
        }
        return count;
    }

    @Override
    public String toString() {
        return Arrays.toString(timeList);
    }
}
