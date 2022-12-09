package org.apache.flink.bilibili.udf.aggregate.windowsum;

import java.util.TreeMap;

/**
 * @author zhangyang
 * @Date:2020/4/21
 * @Time:3:32 PM
 */
public class SumAccumulator {

    private TreeMap<Long, Long> timestampAddNumberMap = new TreeMap<>();
    private long sum;


    public void add(long addNumber, Long dataTime, Integer windowSizeMs, long curMills) {

        timestampAddNumberMap.put(dataTime, timestampAddNumberMap.getOrDefault(dataTime, 0l) + addNumber);
        sum += addNumber;

        while (timestampAddNumberMap.size() > 0 && timestampAddNumberMap.firstKey() + windowSizeMs < curMills) {
            sum -= timestampAddNumberMap.remove(timestampAddNumberMap.firstKey());
        }

    }

    public long getValue() {
        return sum;
    }

}
