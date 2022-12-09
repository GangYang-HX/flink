/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

public class KafkaMetricConfig {

    // Constants
    public static final String FETCH_RATE_GAUGE = "fetch-rate";
    public static final String FETCH_SIZE_AVG_GAUGE = "fetch-size-avg";
    public static final String RECORDS_LAG_MAX_GAUGE = "records-lag-max";
    public static final String FETCH_LATENCY_AVG_GAUGE = "fetch-latency-avg";

    public boolean contains(String name) {
        return name.equals(FETCH_LATENCY_AVG_GAUGE) || name.equals(FETCH_RATE_GAUGE) || name.equals(FETCH_SIZE_AVG_GAUGE) || name.equals(RECORDS_LAG_MAX_GAUGE);
    }
}
