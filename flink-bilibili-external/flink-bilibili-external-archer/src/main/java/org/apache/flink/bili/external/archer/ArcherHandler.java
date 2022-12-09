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

package org.apache.flink.bili.external.archer;

import org.apache.flink.bili.external.archer.integrate.ArcherCreateDummyJobResp;
import org.apache.flink.bili.external.archer.integrate.ArcherGetJobIdByRelateUIDResp;
import org.apache.flink.bili.external.archer.integrate.ArcherQueryInstanceResp;
import org.apache.flink.bili.external.archer.integrate.ArcherUpdateInstanceSuccessResp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import net.dongliu.requests.Requests;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.bili.external.archer.constant.ArcherConstants.ACCOUNT;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.API_CREATE_JOB;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.API_GET_JOB_ID_BY_RELATE_UID;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.API_QUERY_INSTANCE;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.API_QUERY_INSTANCE_BY_RELATE_UID;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.API_SUCCESS_INSTANCE;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.APP_ID;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.GROUP_NAME;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.INSTANCE_GROUP_NAME;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.SECRET_KEY;
import static org.apache.flink.bili.external.archer.constant.ArcherConstants.URL;
import static org.apache.flink.bili.external.archer.utils.DataFormatUtils.getCreateDummyData;
import static org.apache.flink.bili.external.archer.utils.DataFormatUtils.getGetJobIdByRelateUIDData;
import static org.apache.flink.bili.external.archer.utils.DataFormatUtils.getInstanceByUidQueryData;
import static org.apache.flink.bili.external.archer.utils.DataFormatUtils.getInstanceQueryData;
import static org.apache.flink.bili.external.archer.utils.DataFormatUtils.getSuccessData;

/** The handler of Archer. */
public class ArcherHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ArcherHandler.class);

    public static String getSignature(
            String appId,
            String groupName,
            String apiName,
            String account,
            String requestId,
            String secretKey,
            String data) {

        String builder =
                appId
                        + groupName
                        + apiName
                        + account
                        + (requestId == null ? "" : requestId)
                        + (data == null ? "" : data)
                        + secretKey;
        return DigestUtils.md5Hex(builder.getBytes(StandardCharsets.UTF_8));
    }

    /** response = "{\"code\":200,\"data\":\"{}\",\"message\":\"Success\",\"traceId\":\"0\"}". */
    public static <T> ArcherMessage<T> commonArcherRequest(
            String groupName,
            String apiName,
            String requestId,
            String data,
            TypeReference<?> type) {
        String response = null;
        ArcherMessage<T> finalResponse = null;
        try {
            response = post(APP_ID, groupName, apiName, ACCOUNT, requestId, SECRET_KEY, URL, data);
            if (StringUtils.isNotBlank(response)) {
                JSONObject jsonObject = JSONObject.parseObject(response);
                String dataStr = jsonObject.getString("data");
                JSONObject object = JSONObject.parseObject(dataStr);
                jsonObject.put("data", object);
                finalResponse = jsonObject.toJavaObject(type);
            }
        } catch (Exception e) {
            LOG.warn("archer parse response {} error for request: {}", response, data, e);
            finalResponse = ArcherMessage.requestFailedResponse();
        }
        LOG.info(
                "archer request: {}, get native response: {}, final response: {}",
                data,
                response,
                finalResponse);

        return finalResponse;
    }

    public static <T> ArcherMessage<T> commonArcherRequestWithRetry(
            String groupName,
            String apiName,
            String requestId,
            String data,
            TypeReference<?> type) {
        ArcherMessage<T> finalResponse;
        Callable<ArcherMessage<T>> callable =
                () -> commonArcherRequest(groupName, apiName, requestId, data, type);
        Retryer<ArcherMessage<T>> retry = buildRetry();
        try {
            finalResponse = retry.call(callable);
            return finalResponse;
        } catch (ExecutionException | RetryException e) {
            LOG.error("common archer request with retry after 3 times error  :", e);
            finalResponse = ArcherMessage.requestFailedResponse();
        }
        LOG.info(
                "archer request with retry after 3 times when return null : {}, final response: {}",
                data,
                finalResponse);
        return finalResponse;
    }

    private static <T> Retryer<ArcherMessage<T>> buildRetry() {
        return RetryerBuilder.<ArcherMessage<T>>newBuilder()
                .retryIfResult(result -> !result.getCode().equals(200))
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(5, TimeUnit.SECONDS))
                .withWaitStrategy(WaitStrategies.fixedWait(3, TimeUnit.SECONDS))
                .build();
    }

    public static String post(
            String appId,
            String groupName,
            String apiName,
            String account,
            String requestId,
            String secretKey,
            String url,
            String data)
            throws Exception {
        String text;

        String signature =
                getSignature(appId, groupName, apiName, account, requestId, secretKey, data);

        JSONObject pyLoad = new JSONObject();
        pyLoad.put("account", account);
        pyLoad.put("apiName", apiName);
        pyLoad.put("appId", appId);
        pyLoad.put("data", data);
        pyLoad.put("groupName", groupName);
        pyLoad.put("requestId", requestId);
        pyLoad.put("signature", signature);
        Map<String, Object> headers = new HashMap<>();
        headers.put("Content-type", "application/json");
        ObjectMapper mapper = new ObjectMapper();
        try {
            text =
                    Requests.post(url)
                            .headers(headers)
                            .body(mapper.writeValueAsString(pyLoad))
                            .send()
                            .readToText();
        } catch (Exception e) {
            LOG.error("archer post failed with request body {} ", data, e);
            throw e;
        }
        return text;
    }

    public static ArcherMessage<ArcherCreateDummyJobResp> createDummyJob(
            String jobName,
            String tableName,
            String relateUid,
            String jobCron,
            String createUser,
            String opUser) {
        String requestId = UUID.randomUUID().toString();
        String data =
                getCreateDummyData(jobName, tableName, relateUid, jobCron, createUser, opUser);

        return commonArcherRequestWithRetry(
                GROUP_NAME,
                API_CREATE_JOB,
                requestId,
                data,
                new TypeReference<ArcherMessage<ArcherCreateDummyJobResp>>() {});
    }

    public static ArcherMessage<ArcherGetJobIdByRelateUIDResp> getJobIdByRelateUID(
            String relateUID) {
        String requestId = UUID.randomUUID().toString();
        String data = getGetJobIdByRelateUIDData(relateUID);
        return commonArcherRequestWithRetry(
                GROUP_NAME,
                API_GET_JOB_ID_BY_RELATE_UID,
                requestId,
                data,
                new TypeReference<ArcherMessage<ArcherGetJobIdByRelateUIDResp>>() {});
    }

    public static ArcherMessage<ArcherQueryInstanceResp> queryInstance(
            String mixId, String bizStartTime, String bizEndTime, String opUser) {
        String requestId = UUID.randomUUID().toString();

        String data = getInstanceQueryData(mixId, bizStartTime, bizEndTime, opUser);

        return commonArcherRequestWithRetry(
                INSTANCE_GROUP_NAME,
                API_QUERY_INSTANCE,
                requestId,
                data,
                new TypeReference<ArcherMessage<ArcherQueryInstanceResp>>() {});
    }

    public static ArcherMessage<ArcherQueryInstanceResp> queryInstanceByRelateUID(
            String relateUID, String bizStartTime, String bizEndTime, String opUser) {
        String requestId = UUID.randomUUID().toString();

        String data = getInstanceByUidQueryData(relateUID, bizStartTime, bizEndTime, opUser);
        LOG.warn(
                "archer post with request body {},relateUID {} ,bizStartTime {},bizEndTime {}, opUser {}, ",
                data,
                relateUID,
                bizStartTime,
                bizEndTime,
                opUser);
        return commonArcherRequestWithRetry(
                INSTANCE_GROUP_NAME,
                API_QUERY_INSTANCE_BY_RELATE_UID,
                requestId,
                data,
                new TypeReference<ArcherMessage<ArcherQueryInstanceResp>>() {});
    }

    public static ArcherMessage<ArcherUpdateInstanceSuccessResp> updatePartitionInstanceSuccess(
            String instanceId, String opUser) {
        String requestId = UUID.randomUUID().toString();
        String data = getSuccessData(instanceId, opUser);

        return commonArcherRequestWithRetry(
                INSTANCE_GROUP_NAME,
                API_SUCCESS_INSTANCE,
                requestId,
                data,
                new TypeReference<ArcherMessage<ArcherUpdateInstanceSuccessResp>>() {});
    }
}
