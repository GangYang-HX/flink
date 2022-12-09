package com.bilibili.flink.metrics.slf4j;

import java.util.Map;
import java.util.Set;

/** LogMeasurementInfo. */
public class LogMeasurementInfo {
    private final String name;
    private final Map<String, String> labels;

    LogMeasurementInfo(String name, Map<String, String> labels) {
        this.name = name;
        this.labels = labels;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void addLabelsMap(Map<String, String> labelsMap) {
        labels.putAll(labelsMap);
    }

    public Set<String> labelKeys() {
        return labels.keySet();
    }

    public String labelsString() {
        StringBuilder builder = new StringBuilder();
        Set<String> keySet = labels.keySet();
        for (String key : keySet) {
            builder.append(key);
            builder.append("=\"");
            writeEscapedLabelValue(builder, labels.get(key));
            builder.append("\",");
        }
        if (builder.length() > 1) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    public String getLabelValue(String labelKey) {
        String labelValue = labels.get(labelKey);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < labelValue.length(); i++) {
            char c = labelValue.charAt(i);
            switch (c) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\"':
                    builder.append("\\\"");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                default:
                    builder.append(c);
            }
        }
        return builder.toString();
    }

    private void writeEscapedLabelValue(StringBuilder builder, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\"':
                    builder.append("\\\"");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                default:
                    builder.append(c);
            }
        }
    }
}
