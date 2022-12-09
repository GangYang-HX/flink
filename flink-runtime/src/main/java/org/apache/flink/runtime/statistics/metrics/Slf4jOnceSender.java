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

package org.apache.flink.runtime.statistics.metrics;

import org.apache.flink.runtime.statistics.AbstractOnceSender;
import org.apache.flink.runtime.statistics.StartupUtils;
import org.apache.flink.runtime.statistics.StatisticsUtils;
import org.apache.flink.runtime.util.MapUtil;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Slf4jOnceSender extends AbstractOnceSender {

	private static final Logger LOG = LoggerFactory.getLogger(Slf4jOnceSender.class);

	private static final Slf4jOnceSender INSTANCE = new Slf4jOnceSender();

	private String traceID;

	public Slf4jOnceSender() {
		init();
	}

	public static Slf4jOnceSender getInstance() {
		return INSTANCE;
	}

	private void init() {
		traceID = systemDimensions.get(TRACE_ID);
	}

	@Override
	public void send(String name, String value, Map<String, String> dimensions) {

		logExecutor.execute(() -> {
			Map<String, String> metadata = new HashMap<>(systemDimensions);
			if (!CollectionUtil.isNullOrEmpty(dimensions)) {
				metadata.putAll(dimensions);
			}
			Map<String, String> filterMap = MapUtil.filterMap(metadata);

			ObjectMapper mapper = new ObjectMapper();
			ObjectNode nodes = mapper.createObjectNode();
			ArrayNode gauge = mapper.createArrayNode();

			ObjectNode node = mapper.createObjectNode();
			node.put(JSON_NAME, name);
			node.put(JSON_VALUE, value);
			node.put(JSON_LABELS, MapUtil.mapToEscapedString(filterMap));

			gauge.add(node);
			nodes.set(JSON_GAUGE, gauge);

			try {
				LOG.info(mapper.writeValueAsString(nodes));
			} catch (JsonProcessingException e) {
				LOG.warn("Json process error,", e);
			}
		});

	}

	@Override
	public void sendStartTracking(StartupUtils.StartupPhase phase, long timestamp) {
		StartupUtils.setTimestampByPhase(traceID, phase, timestamp);
	}

	@Override
	public void sendEndTracking(StartupUtils.StartupPhase phase, boolean isSucceeded, Map<String, String> dimensions) {
		if (MapUtils.isEmpty(dimensions)) {
			dimensions = new HashMap<>();
		}
		dimensions.put(StatisticsUtils.SUCCESS_STATUS, String.valueOf(isSucceeded));
		dimensions.put(StatisticsUtils.PHASE, phase.name());
		Object startTimestamp = StartupUtils.getTimestampByPhase(traceID, phase);
		if(startTimestamp != null){
			long endTimestamp = System.currentTimeMillis();
			long timeMs = endTimestamp - (long) startTimestamp;
			dimensions.put(START_TIME, String.valueOf(startTimestamp));
			dimensions.put(END_TIME, String.valueOf(endTimestamp));
			send(StatisticsUtils.STARTUP_TIME, timeMs, dimensions);
			StartupUtils.removeTimestampByPhase(traceID, phase);
		}
	}

}
