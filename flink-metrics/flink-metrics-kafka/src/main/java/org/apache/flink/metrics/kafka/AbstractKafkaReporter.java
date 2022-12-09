/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.metrics.kafka.KafkaReporterOptions.FILTER_LABEL_VALUE_CHARACTER;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.LABEL_FILTER;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.MAX_LABEL_VALUE_LENGTH;
import static org.apache.flink.metrics.kafka.KafkaReporterOptions.METRIC_FILTER;

/**
 * base kafka  metrics reporter for kafkaReporter.
 */
@PublicEvolving
public abstract class AbstractKafkaReporter implements MetricReporter {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final Map<Gauge<?>, KafkaMetricsInfo> gauges = new HashMap<>();
	protected final Map<Counter, KafkaMetricsInfo> counters = new HashMap<>();
	protected final Map<Histogram, KafkaMetricsInfo> histograms = new HashMap<>();
	protected final Map<Meter, KafkaMetricsInfo> meters = new HashMap<>();

	private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
	private static final CharacterFilter CHARACTER_FILTER =
		new CharacterFilter() {
			@Override
			public String filterCharacters(String input) {
				return replaceInvalidChars(input);
			}
		};

	private static final char SCOPE_SEPARATOR = '_';
//	private static final String SCOPE_PREFIX = "flink" + SCOPE_SEPARATOR;

	@VisibleForTesting
	static String replaceInvalidChars(final String input) {
		// https://prometheus.io/docs/instrumenting/writing_exporters/
		// Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
		// an underscore.
		String convertedValue = UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
		return maxLabelValueLength > convertedValue.length()
			? convertedValue
			: StringUtils.substring(convertedValue, 0, maxLabelValueLength / 2)
			+ StringUtils.substring(
			convertedValue,
			convertedValue.length() - maxLabelValueLength / 2,
			convertedValue.length());
	}

	private static CharacterFilter labelValueCharactersFilter = CHARACTER_FILTER;

	private static Integer maxLabelValueLength;

	private Filter metricFilter;

	private Filter labelFilter;

	@Override
	public void open(MetricConfig config) {
		metricFilter = new Filter(config.getString(METRIC_FILTER.key(), METRIC_FILTER.defaultValue()));
		labelFilter = new Filter(config.getString(LABEL_FILTER.key(), LABEL_FILTER.defaultValue()));

		boolean filterLabelValueCharacters =
			config.getBoolean(
				FILTER_LABEL_VALUE_CHARACTER.key(),
				FILTER_LABEL_VALUE_CHARACTER.defaultValue());

		maxLabelValueLength =
			config.getInteger(
				MAX_LABEL_VALUE_LENGTH.key(), MAX_LABEL_VALUE_LENGTH.defaultValue());

		if (!filterLabelValueCharacters) {
			labelValueCharactersFilter = input -> input;
		}
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final KafkaMetricsInfo metricInfo = getMetricInfo(metricName, group);

		if (!metricFilter.containKey(metricInfo.getName())) {
			return;
		}

		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, metricInfo);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, metricInfo);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, metricInfo);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, metricInfo);
			} else {
				log.warn(
					"Cannot add unknown metric type {}. This indicates that the reporter "
						+ "does not support this metric type.",
					metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.remove(metric);
			} else if (metric instanceof Gauge) {
				gauges.remove(metric);
			} else if (metric instanceof Histogram) {
				histograms.remove(metric);
			} else if (metric instanceof Meter) {
				meters.remove(metric);
			} else {
				log.warn(
					"Cannot remove unknown metric type {}. This indicates that the reporter "
						+ "does not support this metric type.",
					metric.getClass().getName());
			}
		}
	}

	public KafkaMetricsInfo getMetricInfo(String metricName, MetricGroup group) {
		return new KafkaMetricsInfo(getScopedName(metricName, group), getTags(group));
	}

	private Map<String, String> getTags(MetricGroup group) {
		Map<String, String> tags = new HashMap<>();
		for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
			String name = variable.getKey().substring(1, variable.getKey().length() - 1);
			if (labelFilter.containKey(name)) {
				String value = labelValueCharactersFilter.filterCharacters(variable.getValue());
				tags.put(name, value);
			}
		}
		return tags;
	}

	private static String getScopedName(String metricName, MetricGroup group) {
		return getLogicalScope(group)
			+ SCOPE_SEPARATOR
			+ CHARACTER_FILTER.filterCharacters(metricName);
	}

//    private static String getLogicalScope(MetricGroup group) {
//        return LogicalScopeProvider.castFrom(group)
//                .getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
//    }

	private static String getLogicalScope(MetricGroup group) {
		return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
	}

	public static class Filter {
		private Set<String> filterSet = new HashSet<>();

		public Filter(String filterStr) {
			if (filterStr == null || filterStr.length() == 0) {
				return;
			}

			Arrays.stream(filterStr.split(","))
				.forEach(
					key -> {
						filterSet.add(key);
					});
		}

		public boolean containKey(String filterName) {
			if (filterSet.contains(filterName)) {
				return true;
			}
			return false;
		}
	}
}
