package com.bilibili.bsql.es.format;

import com.alibaba.fastjson.JSON;
import com.bilibili.bsql.common.format.BatchOutputFormat;
import com.bilibili.bsql.common.global.SymbolsConstant;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className EsOutputFormat.java
 * @description This is the description of EsOutputFormat.java
 * @createTime 2020-10-28 11:22:00
 */
public class EsOutputFormat extends BatchOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(EsOutputFormat.class);
    public static final String UTF_8 = "UTF-8";
    public static final String SLASH = "/";
    public static final String POST = "POST";
    private static final String CONTENT_TYPE = "application/x-ndjson";
    private static final String DATE_PATTERN_STR = "<%=([^>]*)%>";
    private static final Pattern DATE_PATTERN = Pattern.compile(DATE_PATTERN_STR);
    private final DynamicTableSink.DataStructureConverter converter;
    private final Map<String, Map<String, String>> INDEX_DATA = new HashMap<>(1);
    private final Map<String, String> ID_DATA = new HashMap<>(1);
    private StringBuilder xJson = new StringBuilder();
    private String[] fieldNames;
    private Class<?>[] fieldTypes;
    private long primaryKey = 0;
    public String typeName = "logs";
    private int taskNumber;
    private String address;
    private RestClient restClient;
    private String matcherItem;
    private String indexName;

    public EsOutputFormat(DataType dataType, DynamicTableSink.Context context) {
        super(dataType,context);
        this.converter = context.createDataStructureConverter(dataType);
    }

    @Override
    public String sinkType() {
        return "es";
    }

    @Override
    public void doOpen(int taskNumber, int numTasks) throws IOException {
        super.doOpen(taskNumber, numTasks);
        this.taskNumber = taskNumber;
        String[] ipHosts = this.address.split(",");
        HttpHost[] httpHosts = new HttpHost[ipHosts.length];
        for (int i = 0; i < ipHosts.length; i++) {
            String[] ipHost = ipHosts[i].split(SymbolsConstant.COLON);
            httpHosts[i] = new HttpHost(ipHost[0], Integer.parseInt(ipHost[1]));
        }
        BasicHeader basicHeader = new BasicHeader("Content-Type", CONTENT_TYPE);
        Header[] defaultHeaders = new Header[]{basicHeader};
        this.restClient = RestClient.builder(httpHosts).setDefaultHeaders(defaultHeaders).build();
        LOG.info("Es client build success. httpHosts = {}", Arrays.asList(httpHosts));
        open();
    }

    public void open() throws IOException {
        Matcher matcher = DATE_PATTERN.matcher(indexName);
        while (matcher.find()) {
            this.matcherItem = matcher.group();
        }
    }

    @Override
    protected void doWrite(RowData record) {
        // attention: 第一次插入数据会形成mapping元数据(类似于字段与类型的对应关系)，以后不允许变更
        Row row = (Row) converter.toExternal(record);
        if (null == row) {
            return;
        }
        Map<String, Object> jsonMap = new HashMap<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            Object obj = row.getField(i);
            if (isJsonObj(obj)) {
                jsonMap.put(fieldNames[i], JSON.parseObject(String.valueOf(obj)));
            } else {
                jsonMap.put(fieldNames[i], obj);
            }
        }
        jsonMap.put("@timestamp", LocalDateTime.now(Clock.systemUTC()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        INDEX_DATA.put("index", ID_DATA);
        ID_DATA.put("_id", taskNumber + "_" + Clock.systemUTC().millis() + "_" + (primaryKey++));
        xJson.append(JSON.toJSON(INDEX_DATA)).append("\n");
        xJson.append(JSON.toJSONString(jsonMap)).append("\n");
    }

    /**
     * 由于flink类型中不存在json
     * 要通过value判断是否为json类型（性能消耗）
     *
     * @param jsonStr
     * @return
     */
    private boolean isJsonObj(Object jsonStr) {
        boolean result = false;
        if (jsonStr instanceof Row) {
            try {
                JSON.parseObject(String.valueOf(jsonStr));
                result = true;
            } catch (Exception ignored) {
            }
        }
        return result;
    }

    @Override
    protected void flush() throws Exception {
        try {
            syncSend();
        } finally {
            xJson = new StringBuilder();
        }
    }

    private void syncSend() throws IOException {
        if (xJson.length() == 0) {
            return;
        }
        String actualIndexName = indexName;
        if (matcherItem != null) {
            actualIndexName = indexName.replaceAll(
                    matcherItem,
                    LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE).replaceAll("-", ".")
            );
        }

        StringEntity stringEntity = new StringEntity(xJson.toString(), UTF_8);
        stringEntity.setContentType(CONTENT_TYPE);

		Response response = restClient.performRequest(POST, SLASH + actualIndexName + SLASH + typeName + SLASH + "_bulk", Collections.emptyMap(), stringEntity);
        int statusCode = response.getStatusLine().getStatusCode();
        if (!isSuccessfulResponse(statusCode)) {
            throw new RuntimeException("not success full response,statusCode:" + statusCode);
        }
    }

    private static boolean isSuccessfulResponse(int statusCode) {
        return statusCode < 300;
    }

    @Override
    public void close() throws IOException {
        super.close();
        restClient.close();
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Class<?>[] getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(Class<?>[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public RestClient getRestClient() {
        return restClient;
    }
}
