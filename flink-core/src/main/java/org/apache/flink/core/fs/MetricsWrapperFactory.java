package org.apache.flink.core.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsWrapperFactory {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsWrapperFactory.class);

	public static <T extends MetricsWrapper> T create(Class<T> clazz) {
		T instance = null;
		try {
			instance = clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.error("init metrics wrapper factory error", e);
		}
		return instance;
	}
}
