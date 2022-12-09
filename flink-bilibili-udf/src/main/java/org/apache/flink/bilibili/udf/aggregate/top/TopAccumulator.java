package org.apache.flink.bilibili.udf.aggregate.top;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.PriorityQueue;


public class TopAccumulator implements Serializable {

    private final static String                       VERTICAL_LINE                   = "_";
    private final static String                       EMPTY_CONTENT                   = "";


    private PriorityQueue<Tuple2<String, Long>> subMetricDimensioQueue = new PriorityQueue<>(new TopAccumulatorComparator());

    public TopAccumulator() {
    }

    public void add(Integer topN, String subDimension, Long subMetric) {
        if (subDimension == null) {
            return;
        }
        if (subMetricDimensioQueue.size() < topN) {
            subMetricDimensioQueue.add(new Tuple2<>(subDimension, subMetric));
        } else {
            if (subMetricDimensioQueue.peek().f1.longValue() < subMetric) {
                subMetricDimensioQueue.poll();
                subMetricDimensioQueue.add(new Tuple2<>(subDimension, subMetric));
            }
        }


    }


    public void retract(Integer topN, String subDimension, Long subMetric) {
        subMetricDimensioQueue.removeIf(item->(item == null || item.f0 == null || item.f0.equals(subDimension)));
    }

    public String getValue() {
        if (subMetricDimensioQueue == null || subMetricDimensioQueue.size() == 0) {
            return EMPTY_CONTENT;
        }

        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Tuple2<String, Long> item : subMetricDimensioQueue) {
            if (count == subMetricDimensioQueue.size() - 1) {
                sb.append(item);
            } else {
                sb.append(item).append(VERTICAL_LINE);
            }
            count++;
        }
        return sb.toString();
    }

    public static class TopAccumulatorComparator implements Comparator<Tuple2<String, Long>> {

        @Override
        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
            if (o1.f1 < o2.f1) {
                return -1;
            }
            if (o1.f1.longValue() == o2.f1.longValue()) {
                return 0;
            }
            return 1;
        }
    }
}
