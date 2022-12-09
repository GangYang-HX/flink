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

package org.apache.flink.bili.external.archer.integrate;

import lombok.Data;

/** Instance query. */
@Data
public class InstanceQuery {
    private Long jobId;
    private String jobName;

    /** Instance ID. */
    private String instanceId;

    private Integer status;
    private Integer jobType;
    private Long projectId;
    private String projectName;
    private String owner;
    private String ownerId;
    private String departmentName;
    private String departmentId;
    private Integer periodType;
    private String bizTime;
    private String runStartTime;
    private String runEndTime;
    private Long costTime;
    private String queueName;
    private Integer priority;
    private String cron;
    private String ctime;
    private String mtime;
    private Integer executeType;
    private String batchName;
    private Long batchId;

    /** Schedule period type: HOUR(0), DAY(1), WEEK(2), MONTH(3). */
    private Integer schedulePeriod;
}
