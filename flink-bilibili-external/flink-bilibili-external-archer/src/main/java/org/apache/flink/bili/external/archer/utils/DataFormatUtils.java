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

package org.apache.flink.bili.external.archer.utils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

/** Utilities of data format. */
public class DataFormatUtils {
    public static final String JOB_TYPE = "jobType";
    public static final String MAX_RETRY_COUNT = "maxRetryCount";
    public static final String JOB_STATUS = "jobStatus";
    public static final String JOB_NAME = "jobName";
    public static final String PERIOD_TYPE = "periodType";
    public static final String CRON = "cron";
    public static final String CREATE_USERNAME = "createUsername";
    public static final String OP_USER = "opUser";
    public static final String RELATE_UID = "relateUID";
    public static final String BIZ_TYPE = "bizType";
    public static final String OUTPUT_TABLE_SET = "outputTableSet";
    public static final String TABLE_NAME = "tableName";
    public static final String MIX_ID = "mixId";
    public static final String BIZ_START_TIME = "bizStartTime";
    public static final String BIZ_END_TIME = "bizEndTime";
    public static final String INSTANCE_ID = "instanceId";

    public static String getCreateDummyData(
            String jobName,
            String tableName,
            String relateUid,
            String jobCron,
            String createUser,
            String opUser) {
        JSONObject tableNameJson = new JSONObject();
        tableNameJson.put(TABLE_NAME, tableName);
        List<Object> jsonList = Lists.newArrayList();
        jsonList.add(tableNameJson);
        JSONArray outPutArray = new JSONArray(jsonList);
        JSONObject createDummyData = new JSONObject();
        createDummyData.put(JOB_TYPE, 70);
        createDummyData.put(MAX_RETRY_COUNT, 2);
        createDummyData.put(JOB_STATUS, 1);
        createDummyData.put(JOB_NAME, jobName);
        createDummyData.put(PERIOD_TYPE, 3);
        createDummyData.put(CRON, jobCron);
        createDummyData.put(CREATE_USERNAME, createUser);
        createDummyData.put(OP_USER, opUser);
        createDummyData.put(RELATE_UID, relateUid);
        createDummyData.put(BIZ_TYPE, 6);
        createDummyData.put(OUTPUT_TABLE_SET, outPutArray);
        return JSON.toJSONString(createDummyData);
    }

    /**
     * attention: bizType must equals with created job's biz type ,otherwise may cause create
     * repeated dummy job.
     *
     * @param relateUID relate UID
     * @return code
     */
    public static String getGetJobIdByRelateUIDData(String relateUID) {
        JSONObject relatedUIDDATA = new JSONObject();
        relatedUIDDATA.put(RELATE_UID, relateUID);
        relatedUIDDATA.put(BIZ_TYPE, 6);
        return JSON.toJSONString(relatedUIDDATA);
    }

    public static String getInstanceQueryData(
            String mixId, String bizStartTime, String bizEndTime, String opUser) {
        JSONObject instanceQueryData = new JSONObject();
        instanceQueryData.put(MIX_ID, mixId);
        instanceQueryData.put(OP_USER, opUser);
        instanceQueryData.put(BIZ_START_TIME, bizStartTime);
        instanceQueryData.put(BIZ_END_TIME, bizEndTime);
        return JSON.toJSONString(instanceQueryData);
    }

    public static String getInstanceByUidQueryData(
            String relateUID, String bizStartTime, String bizEndTime, String opUser) {
        JSONObject instanceByUidQueryData = new JSONObject();
        instanceByUidQueryData.put(RELATE_UID, relateUID);
        instanceByUidQueryData.put(BIZ_START_TIME, bizStartTime);
        instanceByUidQueryData.put(BIZ_END_TIME, bizEndTime);
        instanceByUidQueryData.put(OP_USER, opUser);
        instanceByUidQueryData.put(BIZ_TYPE, 8);
        return instanceByUidQueryData.toString();
    }

    public static String getSuccessData(String instanceId, String opUser) {
        JSONObject successData = new JSONObject();
        successData.put(INSTANCE_ID, instanceId);
        successData.put(OP_USER, opUser);
        return JSON.toJSONString(successData);
    }
}
