package com.bilibili.flink.metrics.slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Dove
 * @Date 2021/8/25 9:57 下午
 */
class LogMeasurementInfoProvider implements LogMetricInfoProvider {
	@VisibleForTesting
	static final char SCOPE_SEPARATOR = '_';

	private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
	private static Integer maxLabelValueLength = 500;

	private static final CharacterFilter CHARACTER_FILTER = input -> replaceInvalidChars(input);

	@VisibleForTesting
	static String replaceInvalidChars(final String input) {
		// https://prometheus.io/docs/instrumenting/writing_exporters/
		// Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to an underscore.
		String convertedValue = UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");

		return maxLabelValueLength > convertedValue.length() ?
			convertedValue :
			StringUtils.substring(convertedValue, 0, maxLabelValueLength / 2) + StringUtils.substring(convertedValue, convertedValue.length() - maxLabelValueLength / 2, convertedValue.length());
	}

	public LogMeasurementInfoProvider() {
	}

	@Override
	public LogMeasurementInfo getMetricInfo(String metricName, MetricGroup group, AbstractLogReporter.Filter labelFilter, CharacterFilter labelValueCharactersFilter) {
		return new LogMeasurementInfo(getScopedName(metricName, group), getLabels(group, labelFilter, labelValueCharactersFilter));
	}

	private static Map<String, String> getLabels(MetricGroup group, AbstractLogReporter.Filter labelFilter, CharacterFilter labelValueCharactersFilter) {
		// Keys are surrounded by brackets: remove them, transforming "<name>" to "name".
		Map<String, String> labels = new HashMap<>();
		for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
			String name = variable.getKey().substring(1, variable.getKey().length() - 1);
			if (labelFilter.containKey(name)) {
				String labelValue = labelValueCharactersFilter.filterCharacters(variable.getValue());
				labels.put(name, labelValue);
			}
		}
		return labels;
	}

	private static String getScopedName(String metricName, MetricGroup group) {
		return CHARACTER_FILTER.filterCharacters(getLogicalScope(group) + SCOPE_SEPARATOR + metricName);
	}

	private static String getLogicalScope(MetricGroup group) {
		return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
	}
}
