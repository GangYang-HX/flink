package com.bilibili.flink.metrics.slf4j;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.Scheduled;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** Slf4jJsonReporter. */
@InstantiateViaFactory(
        factoryClassName = "com.bilibili.flink.metrics.slf4j.Slf4jJsonReporterFactory")
public class Slf4jJsonReporter extends AbstractLogReporter implements Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(Slf4jJsonReporter.class);

    private static final String LABEL_INDEX_PREFIX = "index";
    private static final String LABEL_KEY_PREFIX = "LK";

    public Slf4jJsonReporter() {
        super(new LogMeasurementInfoProvider());
    }

    @Override
    public void open(MetricConfig metricConfig) {
        super.open(metricConfig);
    }

    @Override
    public void close() {}

    @Override
    public void report() {
        try {
            tryReport();
        } catch (ConcurrentModificationException ignored) {
            // at tryReport() we don't synchronize while iterating over the various maps which might
            // cause a
            // ConcurrentModificationException to be thrown, if concurrently a metric is being added
            // or removed.
        }
    }

    @Nullable
    private void tryReport() {
        try {

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode nodes = mapper.createObjectNode();

            ArrayNode gauge = mapper.createArrayNode();
            ArrayNode counter = mapper.createArrayNode();
            ArrayNode summary = mapper.createArrayNode();
            ArrayNode histogram = mapper.createArrayNode();

            Map<String, String> labelIndexMap = new HashMap<>();
            Map<String, String> labelKVMap = new HashMap<>();

            AtomicLong atomicLabelIndex = new AtomicLong(0);
            AtomicLong atomicLabelKV = new AtomicLong(0);

            for (Map.Entry<Gauge<?>, LogMeasurementInfo> entry : gauges.entrySet()) {
                ObjectNode node = mapper.createObjectNode();
                String labelIndex =
                        getLabelIndex(
                                entry.getValue(),
                                labelIndexMap,
                                labelKVMap,
                                atomicLabelIndex,
                                atomicLabelKV);
                node.put("name", entry.getValue().getName());
                node.put("labels", labelIndex);
                node.put("value", String.valueOf(entry.getKey().getValue()));
                gauge.add(node);
            }
            nodes.set("gauge", gauge);

            for (Map.Entry<Counter, LogMeasurementInfo> entry : counters.entrySet()) {
                ObjectNode node = mapper.createObjectNode();
                String labelIndex =
                        getLabelIndex(
                                entry.getValue(),
                                labelIndexMap,
                                labelKVMap,
                                atomicLabelIndex,
                                atomicLabelKV);
                node.put("name", entry.getValue().getName());
                node.put("labels", labelIndex);
                node.put("value", entry.getKey().getCount());
                counter.add(node);
            }
            nodes.set("counter", counter);

            for (Map.Entry<Histogram, LogMeasurementInfo> entry : histograms.entrySet()) {
                HistogramStatistics stats = entry.getKey().getStatistics();
                String name = entry.getValue().getName();
                String labelIndex =
                        getLabelIndex(
                                entry.getValue(),
                                labelIndexMap,
                                labelKVMap,
                                atomicLabelIndex,
                                atomicLabelKV);
                histogram.add(
                        createHistogramJson(mapper, name + "_count", labelIndex, stats.size()));
                histogram.add(
                        createHistogramJson(mapper, name + "_min", labelIndex, stats.getMin()));
                histogram.add(
                        createHistogramJson(mapper, name + "_max", labelIndex, stats.getMax()));
                histogram.add(
                        createHistogramJson(mapper, name + "_mean", labelIndex, stats.getMean()));
            }
            nodes.set("histogram", histogram);

            for (Map.Entry<Meter, LogMeasurementInfo> entry : meters.entrySet()) {
                ObjectNode node = mapper.createObjectNode();
                String labelIndex =
                        getLabelIndex(
                                entry.getValue(),
                                labelIndexMap,
                                labelKVMap,
                                atomicLabelIndex,
                                atomicLabelKV);
                node.put("name", entry.getValue().getName());
                node.put("labels", labelIndex);
                node.put("value", entry.getKey().getRate());
                summary.add(node);
            }
            nodes.set("summary", summary);

            ObjectNode labelNode = mapper.createObjectNode();
            for (String labelIndexValue : labelIndexMap.keySet()) {
                String labelIndex = labelIndexMap.get(labelIndexValue);
                labelNode.put(labelIndex, labelIndexValue);
            }
            nodes.set("labelIndex", labelNode);

            ObjectNode labelKVNode = mapper.createObjectNode();
            for (String labelValue : labelKVMap.keySet()) {
                String labelKey = labelKVMap.get(labelValue);
                labelKVNode.put(labelKey, labelValue);
            }
            nodes.set("labelKV", labelKVNode);

            LOG.info(mapper.writeValueAsString(nodes));
        } catch (JsonProcessingException e) {
        }
    }

    private String getLabelIndex(
            LogMeasurementInfo logMeasurementInfo,
            Map<String, String> labelIndexMap,
            Map<String, String> labelKVMap,
            AtomicLong atomicLabelIndex,
            AtomicLong atomicLabelKV) {
        String labelIndexValue = getLabelIndexValue(logMeasurementInfo, labelKVMap, atomicLabelKV);
        if (labelIndexValue == "") {
            return "";
        }
        String labelIndex = labelIndexMap.get(labelIndexValue);
        if (labelIndex == null) {
            String newLabelIndex = LABEL_INDEX_PREFIX + atomicLabelIndex.getAndIncrement();
            labelIndexMap.put(labelIndexValue, newLabelIndex);
            return newLabelIndex;
        } else {
            return labelIndex;
        }
    }

    private String getLabelIndexValue(
            LogMeasurementInfo logMeasurementInfo,
            Map<String, String> labelKVMap,
            AtomicLong atomicLabelKV) {
        Set<String> labelKeys = logMeasurementInfo.labelKeys();
        StringBuilder builder = new StringBuilder();
        for (String labelKey : labelKeys) {
            String labelValue = logMeasurementInfo.getLabelValue(labelKey);
            StringBuilder sbKey = new StringBuilder();
            sbKey.append("\"").append(labelKey).append("\"=\"").append(labelValue).append("\"");
            String k = sbKey.toString();
            String labelKVIndex = labelKVMap.get(k);
            if (labelKVIndex == null) {
                String newLabelKVIndex = LABEL_KEY_PREFIX + atomicLabelKV.getAndIncrement();
                labelKVMap.put(k, newLabelKVIndex);
                labelKVIndex = newLabelKVIndex;
            }
            builder.append(labelKVIndex).append(",");
        }
        if (builder.length() > 0) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private ObjectNode createHistogramJson(
            ObjectMapper mapper, String name, String labels, Object value) {
        ObjectNode node = mapper.createObjectNode();
        node.put("name", name);
        node.put("labels", labels);
        node.put("value", String.valueOf(value));
        return node;
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }
}
