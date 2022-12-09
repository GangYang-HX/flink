package org.apache.flink.bilibili.udf.aggregate.windowsumtop;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.bilibili.udf.aggregate.ValueWithTimestamp;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author zhangyang
 * @Date:2020/5/6
 * @Time:4:01 PM
 */
public class SumTopAccumulator {

    private final static String                       VERTICAL_LINE                   = "|";
    private final static String                       COLON                           = ":";
    private final static String                       EMPTY_CONTENT                   = "";

    // Key : subKey with timestamp; Value : addNumber
    private TreeMap<ValueWithTimestamp<String>, Long> subKeyWithTimestampAddNumberMap = new TreeMap<>(
                                                                                                      new TopSumAccumulatorComparator());

    // Key: subKey ; Value : Tuple2.f0 : sum of addNumber, Tuple2.f1 : count of subKey
    private HashMap<String, Tuple2<Long, Long>> subKeyAccDetailMap = new HashMap<>();

    public SumTopAccumulator() {
    }

    public void add(Integer topN, String subKey, Long addNumber, Long dataTime, Integer windowSizeMs, Long curMills) {

        ValueWithTimestamp<String> handleKey = new ValueWithTimestamp<String>(dataTime, subKey);

        subKeyWithTimestampAddNumberMap.put(handleKey, subKeyWithTimestampAddNumberMap.getOrDefault(handleKey, 0l)
                                                       + addNumber);

        Tuple2<Long, Long> tuple = subKeyAccDetailMap.getOrDefault(subKey, new Tuple2<Long, Long>(0l, 0l));
        tuple.f1++;
        tuple.f0 += addNumber;
        subKeyAccDetailMap.put(subKey, tuple);

        while (subKeyWithTimestampAddNumberMap.size() > 0
               && subKeyWithTimestampAddNumberMap.firstKey().getEventTime() + windowSizeMs < curMills) {

            ValueWithTimestamp<String> firstKey = subKeyWithTimestampAddNumberMap.firstKey();
            Long removeValue = subKeyWithTimestampAddNumberMap.remove(firstKey);
            if (subKeyAccDetailMap.get(firstKey.getValue()).f1 == 1) {
                subKeyAccDetailMap.remove(firstKey.getValue());
            } else {
                subKeyAccDetailMap.get(firstKey.getValue()).f0 -= removeValue;
                subKeyAccDetailMap.get(firstKey.getValue()).f1--;
            }

        }

        if (subKeyWithTimestampAddNumberMap.size() > topN) {

            ValueWithTimestamp<String> minKey = null;
            long minValue = Long.MAX_VALUE;

            for (Map.Entry<ValueWithTimestamp<String>, Long> entry : subKeyWithTimestampAddNumberMap.entrySet()) {
                if (entry.getValue() < minValue) {
                    minValue = entry.getValue();
                    minKey = entry.getKey();
                }
            }

            subKeyWithTimestampAddNumberMap.remove(minKey);

            if (subKeyAccDetailMap.get(minKey.getValue()).f1 == 1) {
                subKeyAccDetailMap.remove(minKey.getValue());
            } else {
                subKeyAccDetailMap.get(minKey.getValue()).f0 -= minValue;
                subKeyAccDetailMap.get(minKey.getValue()).f1--;
            }
        }
    }

    public String getValue() {
        if (subKeyAccDetailMap == null || subKeyAccDetailMap.size() == 0) {
            return EMPTY_CONTENT;
        }
        StringBuilder ret = new StringBuilder();
        int count = 0;
        for (String item : subKeyAccDetailMap.keySet()) {
            if (count == subKeyAccDetailMap.size() - 1) {
                ret.append(item + COLON + subKeyAccDetailMap.get(item).f0);
            } else {
                ret.append(item + COLON + subKeyAccDetailMap.get(item).f0).append(VERTICAL_LINE);
            }
            count++;
        }
        return ret.toString();
    }

    public static class TopSumAccumulatorComparator implements Comparator<ValueWithTimestamp<String>> {

        @Override
        public int compare(ValueWithTimestamp<String> o1, ValueWithTimestamp<String> o2) {
            if (!o1.getEventTime().equals(o2.getEventTime())) {
                return (int) (o1.getEventTime() - o2.getEventTime());
            }

            if (!o1.getValue().equals(o2.getValue())) {
                return o1.getValue().compareTo(o2.getValue());
            }
            return 0;

        }
    }
}
