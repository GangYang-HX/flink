package org.apache.flink.bili.external.keeper;

import org.apache.flink.bili.external.keeper.dto.req.KeeperApiReq;
import org.apache.flink.bili.external.keeper.dto.resp.KeeperGetTableInfoResp;

import cn.hutool.core.lang.TypeReference;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.bapis.datacenter.service.keeper.DbType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.dongliu.requests.Requests;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.flink.bili.external.keeper.constant.KeeperConstants.ACCOUNT;
import static org.apache.flink.bili.external.keeper.constant.KeeperConstants.API_GET_TABLE_INFO;
import static org.apache.flink.bili.external.keeper.constant.KeeperConstants.APP_ID;
import static org.apache.flink.bili.external.keeper.constant.KeeperConstants.GROUP_NAME_KEEPER;
import static org.apache.flink.bili.external.keeper.constant.KeeperConstants.SECRET_KEY;
import static org.apache.flink.bili.external.keeper.constant.KeeperConstants.URL;

/** wrapper the method for keeper by open api. */
@Slf4j
public class KeeperHandler {

    private static final String OPEN_API_ACCOUNT = "account";
    private static final String OPEN_API_METHOD_NAME = "apiName";
    private static final String OPEN_API_APP_ID = "appId";
    private static final String OPEN_API_REQ_JSON = "data";
    private static final String OPEN_API_GROUP_NAME = "groupName";
    private static final String OPEN_API_PER_REQ_UUID = "requestId";
    private static final String OPEN_API_SIGNATURE = "signature";

    private static final String GET_TABLE_INFO_PARAM_DB_TYPE = "dbType";
    private static final String GET_TABLE_INFO_PARAM_DS_NAME = "dataServiceName";
    private static final String GET_TABLE_INFO_PARAM_DB_NAME = "databaseName";
    private static final String GET_TABLE_INFO_PARAM_TAB_NAME = "tableName";

    private static final Integer RETRY_TIMES = 3;
    private static final Integer SOCKET_TIME_OUT = 6 * 1000;
    private static final Integer CONNECT_TIME_OUT = 6 * 1000;

    /**
     * get table meta info like bilicatalog
     *
     * @param datasource
     * @param databaseName
     * @param tableName
     * @return
     * @throws Exception
     */
    public static KeeperMessage<KeeperGetTableInfoResp> getTableInfo(
            String datasource, String databaseName, String tableName) throws Exception {
        Integer dbTypeCode = getDbType(datasource);
        if (dbTypeCode == DbType.Unknown_VALUE) {
            throw new Exception("unSupported datasource type:" + datasource);
        }
        String data =
                getTableInfoData(
                        dbTypeCode,
                        datasource,
                        databaseName == null ? "" : databaseName,
                        tableName);
        return commonKeeperRequestWithRetry(
                RETRY_TIMES,
                GROUP_NAME_KEEPER,
                API_GET_TABLE_INFO,
                data,
                new TypeReference<KeeperMessage<KeeperGetTableInfoResp>>() {});
    }

    private static <T> KeeperMessage<T> commonKeeperRequestWithRetry(
            Integer retryTimes,
            String groupName,
            String apiName,
            String data,
            TypeReference<?> type)
            throws Exception {
        Function<KeeperApiReq, KeeperMessage<T>> function = KeeperHandler::commonKeeperRequest;
        KeeperApiReq req =
                KeeperApiReq.builder()
                        .groupName(groupName)
                        .apiName(apiName)
                        .requestId(UUID.randomUUID().toString())
                        .data(data)
                        .type(type)
                        .build();
        return funcRetry(retryTimes, function, req);
    }

    private static <T> KeeperMessage<T> commonKeeperRequest(KeeperApiReq req) {
        String response;
        KeeperMessage<T> finalResponse;
        try {
            TypeReference<?> type = req.getType();
            String requestId = UUID.randomUUID().toString();
            response =
                    post(
                            APP_ID,
                            req.getGroupName(),
                            req.getApiName(),
                            ACCOUNT,
                            requestId,
                            SECRET_KEY,
                            URL,
                            req.getData());
            if (StringUtils.isNotBlank(response)) {
                JSONObject jsonObject = JSONUtil.parseObj(response);
                String data = jsonObject.getStr("data");
                JSONObject object = JSONUtil.parseObj(data);
                jsonObject.put("data", object);
                finalResponse = jsonObject.toBean(type.getType());
            } else {
                throw new RuntimeException("keeper return response is empty");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info(
                "keeper request: {}, get native response: {}, final response: {}",
                req.getData(),
                response,
                finalResponse);

        return finalResponse;
    }

    private static String post(
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
        pyLoad.put(OPEN_API_ACCOUNT, account);
        pyLoad.put(OPEN_API_METHOD_NAME, apiName);
        pyLoad.put(OPEN_API_APP_ID, appId);
        pyLoad.put(OPEN_API_REQ_JSON, data);
        pyLoad.put(OPEN_API_GROUP_NAME, groupName);
        pyLoad.put(OPEN_API_PER_REQ_UUID, requestId);
        pyLoad.put(OPEN_API_SIGNATURE, signature);
        Map<String, Object> headers = new HashMap<>();
        headers.put("Content-type", "application/json");
        ObjectMapper mapper = new ObjectMapper();
        try {
            text =
                    Requests.post(url)
                            .headers(headers)
                            .body(mapper.writeValueAsString(pyLoad))
                            .socksTimeout(SOCKET_TIME_OUT)
                            .connectTimeout(CONNECT_TIME_OUT)
                            .send()
                            .readToText();
        } catch (Exception e) {
            log.error("keeper post failed with request body {} ", data, e);
            throw e;
        }
        return text;
    }

    /**
     * Simple retry logic
     *
     * @param retryTimes
     * @param func
     * @param param
     * @param <T>
     * @param <R>
     * @return
     * @throws Exception
     */
    private static <T, R> R funcRetry(int retryTimes, Function<T, R> func, T param)
            throws Exception {
        int i = 0;
        Exception finalException;
        R result = null;
        do {
            try {
                finalException = null;
                result = func.apply(param);
                break;
            } catch (Exception e) {
                finalException = e;
                Thread.sleep((i + 1) * 500);
            }
        } while (i++ < retryTimes);
        if (finalException != null) {
            throw finalException;
        }
        return result;
    }

    private static String getSignature(
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

    private static String getTableInfoData(
            Integer dbTypeCode, String dataServiceName, String databaseName, String tableName) {
        JSONObject req = new JSONObject();
        req.put(GET_TABLE_INFO_PARAM_DB_TYPE, dbTypeCode);
        req.put(GET_TABLE_INFO_PARAM_DS_NAME, dataServiceName);
        req.put(GET_TABLE_INFO_PARAM_DB_NAME, databaseName);
        req.put(GET_TABLE_INFO_PARAM_TAB_NAME, tableName);
        return JSONUtil.toJsonStr(req);
    }

    private static Integer getDbType(String dataSource) {
        String dataType = dataSource.split("_")[0];
        DbType dbType;
        try {
            dbType = DbType.valueOf(dataType);
        } catch (IllegalArgumentException e) {
            dbType = DbType.Unknown;
        }
        return dbType.getNumber();
    }
}
