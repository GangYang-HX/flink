package org.apache.flink.bilibili.udf.aggregate.windowtop;


import org.apache.flink.bilibili.udf.aggregate.ValueWithTimestamp;

import java.io.Serializable;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * @author zhangyang
 * @Date:2020/4/20
 * @Time:3:40 PM
 */
public class TopAccumulator implements Serializable {

    private final static String                       VERTICAL_LINE                   = "|";
    private final static String                       EMPTY_CONTENT                   = "";
    private TreeMap<Long, ValueWithTimestamp<String>> addNumberSubkeyWithTimestampMap = new TreeMap<>(
                                                                                                      new TopAccumulatorComparator());

    public TopAccumulator() {
    }

    public void add(Integer topN, String value, Long sortKey, Long dataTime, Integer windowSizeMs, Long curMills) {
        addNumberSubkeyWithTimestampMap.put(sortKey, new ValueWithTimestamp<>(dataTime, value));
        addNumberSubkeyWithTimestampMap.entrySet().removeIf(entry -> (entry.getValue().getEventTime() + windowSizeMs) < curMills);
        if (addNumberSubkeyWithTimestampMap.size() > topN) {
            addNumberSubkeyWithTimestampMap.remove(addNumberSubkeyWithTimestampMap.firstKey());
        }
    }

    public String getValue() {
        if (addNumberSubkeyWithTimestampMap == null || addNumberSubkeyWithTimestampMap.size() == 0) {
            return EMPTY_CONTENT;
        }

        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (ValueWithTimestamp<String> item : addNumberSubkeyWithTimestampMap.values()) {
            if (count == addNumberSubkeyWithTimestampMap.size() - 1) {
                sb.append(item.getValue());
            } else {
                sb.append(item.getValue()).append(VERTICAL_LINE);
            }
            count++;
        }
        return sb.toString();
    }

    public static class TopAccumulatorComparator implements Comparator<Long> {

        @Override
        public int compare(Long o1, Long o2) {
            if (o1 < o2) {
                return -1;
            }
            if (o1.longValue() == o2.longValue()) {
                return 0;
            }
            return 1;
        }
    }
}
