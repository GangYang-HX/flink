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

package org.apache.flink.bili.external.archer.constant;

import java.time.format.DateTimeFormatter;

/** Archer constants including authorization, saber, http api, time pattern and Flink etc. */
public class ArcherConstants {
    /** Authorization. */
    public static final String APP_ID = "datacenter.archer.archer-bypass";

    public static final String SECRET_KEY = "8bd7931b92afcbb189df55c08ef3ca8d";
    //	public static final String SECRET_KEY = "5fe71b35457f10fc30ac583206cf8d28";
    public static final String URL = "http://berserker.bilibili.co/voyager/v1/invocation/grpc";
    //	public static final String URL =
    // "http://uat-berserker.bilibili.co/voyager/v1/invocation/grpc";
    public static final String ACCOUNT = "p_flink_hdfsSink";
    //	public static final String ACCOUNT = "p_archer";
    public static final String USER = "archer_lancer_dummy";

    /** Saber. */
    public static final String SYSTEM_USER_ID = "hdfs.partition.commit.user";

    public static final String LANCER_LOG_ID = "lancer.log.id";

    /** HTTP API interfaces. */
    public static final String GROUP_NAME = "JobManager";

    public static final String INSTANCE_GROUP_NAME = "InstanceManager";
    public static final String API_CREATE_JOB = "createJob";
    public static final String API_GET_JOB_ID_BY_RELATE_UID = "getJobIdByRelateUID";
    public static final String API_QUERY_INSTANCE = "queryInstance";
    public static final String API_QUERY_INSTANCE_BY_RELATE_UID = "queryInstanceByRelateUId";
    public static final String API_SUCCESS_INSTANCE = "dummyJobExecSuccess";

    /** HTTP API parameters. */
    public static final String JOB_CRON_LOG_HOUR = "0 0 */1 * * ?";

    public static final String JOB_CRON_LOG_DAY = "15 0 0 * * ?";

    /** Time pattern. */
    public static final String DAY_PATTERN = "yyyyMMdd";

    public static final String HOUR_PATTERN = "yyyyMMdd/HH";
    public static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern(DAY_PATTERN);
    public static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern(HOUR_PATTERN);

    /** Flink. */
    public static final String SINK_DATABASE_NAME_KEY = "sink.database.name";

    public static final String SINK_TABLE_NAME_KEY = "sink.table.name";
    public static final String SINK_PARTITION_KEY = "sink.partition.key";
}
