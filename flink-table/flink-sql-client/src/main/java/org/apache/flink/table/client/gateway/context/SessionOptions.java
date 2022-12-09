package org.apache.flink.table.client.gateway.context;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/** session options. */
public class SessionOptions {
    public static final ConfigOption<String> DEFAULT_CATALOG_NAME =
            ConfigOptions.key("session.default.catalog.name")
                    .stringType()
                    .defaultValue("generic_in_memory");

    public static final ConfigOption<Map<String, String>> DEFAULT_CATALOG_OPTIONS =
            ConfigOptions.key("session.default.catalog.options").mapType().defaultValue(emptyMap());

    public static final ConfigOption<List<String>> EXTRA_MODULES =
            ConfigOptions.key("session.extra.modules").stringType().asList().noDefaultValue();
}
