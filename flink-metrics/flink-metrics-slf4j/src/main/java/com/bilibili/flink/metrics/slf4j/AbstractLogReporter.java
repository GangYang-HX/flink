package com.bilibili.flink.metrics.slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.bilibili.flink.metrics.slf4j.Slf4jJsonReporterOptions.LABEL_FILTER;
import static com.bilibili.flink.metrics.slf4j.Slf4jJsonReporterOptions.METRIC_FILTER;
import static com.bilibili.flink.metrics.slf4j.Slf4jJsonReporterOptions.FILTER_LABEL_VALUE_CHARACTER;
import static com.bilibili.flink.metrics.slf4j.Slf4jJsonReporterOptions.MAX_LABEL_VALUE_LENGTH;

/**
 * @author Dove
 * @Date 2021/8/25 9:55 下午
 */
@PublicEvolving
public abstract class AbstractLogReporter implements MetricReporter, CharacterFilter {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final Map<Gauge<?>, LogMeasurementInfo> gauges = new HashMap<>();
	protected final Map<Counter, LogMeasurementInfo> counters = new HashMap<>();
	protected final Map<Histogram, LogMeasurementInfo> histograms = new HashMap<>();
	protected final Map<Meter, LogMeasurementInfo> meters = new HashMap<>();

	protected final LogMetricInfoProvider logMetricInfoProvider;

	private Filter metricFilter;
	private Filter labelFilter;

	private static Integer maxLabelValueLength = 500;

	private static final CharacterFilter CHARACTER_FILTER = input -> replaceInvalidChars(input);

	private CharacterFilter labelValueCharactersFilter = CHARACTER_FILTER;

	//eg:-yD metrics.reporter.prom.labels="saber_job_name=platform_custom_job_name;saber_job_owner=yanggang01@bilibili.com"
	private Map<String, String> labels;
	private static final String ARG_LABEL = "labels";

	protected AbstractLogReporter(LogMetricInfoProvider logMetricInfoProvider) {
		this.logMetricInfoProvider = logMetricInfoProvider;
	}

	@Override
	public void open(MetricConfig config) {
		metricFilter = new Filter(config.getString(METRIC_FILTER.key(), METRIC_FILTER.defaultValue()));
		labelFilter = new Filter(config.getString(LABEL_FILTER.key(), LABEL_FILTER.defaultValue()));
		labels = parseLabels(config.getString(ARG_LABEL, ""));

		boolean filterLabelValueCharacters = config.getBoolean(FILTER_LABEL_VALUE_CHARACTER.key(), FILTER_LABEL_VALUE_CHARACTER.defaultValue());

		if (!filterLabelValueCharacters) {
			labelValueCharactersFilter = input -> input;
		}

		maxLabelValueLength = config.getInteger(MAX_LABEL_VALUE_LENGTH.key(), MAX_LABEL_VALUE_LENGTH.defaultValue());
		log.debug("AbstractLogReporter: set the label value length to {}", maxLabelValueLength);
	}

	@VisibleForTesting
	static String replaceInvalidChars(final String input) {
		return maxLabelValueLength > input.length() ?
			input :
			StringUtils.substring(input, 0, maxLabelValueLength / 2) + StringUtils.substring(input, input.length() - maxLabelValueLength / 2, input.length());
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final LogMeasurementInfo logMetricInfo = logMetricInfoProvider.getMetricInfo(metricName, group, labelFilter, labelValueCharactersFilter);
		// todo filter
		if (!metricFilter.containKey(logMetricInfo.getName())) {
			return;
		}

		if (!labels.isEmpty()) {
			logMetricInfo.addLabelsMap(labels);
		}

		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, logMetricInfo);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, logMetricInfo);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, logMetricInfo);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, logMetricInfo);
			} else {
				log.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}

		log.debug("add metric({}) success.", logMetricInfo.getName());
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
				log.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	protected Map<String, String> parseLabels(final String labelConfig) {
		Map<String, String> labels = new HashMap<>();

		if (!labelConfig.isEmpty()) {
			String[] kvs = labelConfig.split(";");
			for (String kv : kvs) {
				int idx = kv.indexOf("=");
				if (idx < 0) {
					log.warn("Invalid label: {}, will be ignored", kv);
					continue;
				}

				String labelKey = kv.substring(0, idx);
				String labelValue = kv.substring(idx + 1);
				if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(labelKey) || org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(labelValue)) {
					log.warn("Invalid label {labelKey:{}, labelValue:{}} must not be empty", labelKey, labelValue);
					continue;
				}
				labels.put(labelKey, labelValue);
			}
		}

		try {
			String hostName = InetAddress.getLocalHost().getHostName();
			labels.put("instance_name", hostName);
		} catch (UnknownHostException e) {
			log.warn("Obtaining hostName(instance_name) failed.");
		}
		return labels;
	}

	public static class Filter {
		private List<Pattern> filter;

		public Filter(String filterStr) {
			if (filterStr == null || filterStr.length() == 0) {
				filter = Collections.emptyList();
			} else {
				filter = Arrays.stream(filterStr.split(","))
					.map(a -> Pattern.compile("^(?i)" + a + "$"))
					.collect(Collectors.toList());
			}
		}

		public boolean containKey(String filterName) {
			for (Pattern pattern : filter) {
				if (pattern.matcher(filterName.toLowerCase(Locale.ROOT)).find()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public String toString() {
			return "Filter{" +
				"filter=" + filter.get(0) +
				'}';
		}
	}
}
