package com.bilibili.flink.metrics.slf4j;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Config options for the {@link Slf4jJsonReporter}. */
@Documentation.SuffixOption
public class Slf4jJsonReporterOptions {

    public static final ConfigOption<String> METRIC_FILTER =
            ConfigOptions.key("metricFilter").defaultValue("").withDescription("Metric filter.");

    public static final ConfigOption<String> LABEL_FILTER =
            ConfigOptions.key("labelFilter").defaultValue("").withDescription("Label filter.");

    public static final ConfigOption<Boolean> FILTER_LABEL_VALUE_CHARACTER =
            ConfigOptions.key("filterLabelValueCharacters")
                    .defaultValue(true)
                    .withDescription("Filter label value characters.");

    public static final ConfigOption<Integer> MAX_LABEL_VALUE_LENGTH =
            ConfigOptions.key("maxLabelValueLen")
                    .defaultValue(500)
                    .withDescription("Specifies the max label value.");
}
