package com.bilibili.flink.metrics.slf4j;

import org.apache.flink.runtime.util.MapUtil;

import java.util.Map;
import java.util.Set;

/**
 * @author Dove
 * @Date 2021/8/25 9:53 下午
 */
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

	public String getLabelValue(String labelKey) {
		String labelValue = labels.get(labelKey);
		return MapUtil.escapedString(labelValue);
	}
}
