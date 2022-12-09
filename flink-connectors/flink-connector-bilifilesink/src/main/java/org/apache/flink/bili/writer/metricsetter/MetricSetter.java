/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.metricsetter;

import org.apache.flink.metrics.MetricGroup;

/**
 *
 * @author zhouxiaogang
 * @version $Id: MetricSetter.java, v 0.1 2020-06-11 19:12
zhouxiaogang Exp $$
 */
public interface MetricSetter {

	void setMetricGroup(MetricGroup group);
}
