package com.bilibili.bsql.hdfs.util;

import com.alibaba.fastjson.JSON;
import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to get hive meta information from keeper
 */
@lombok.Data
public class HiveMetaWrapper {
    private static final Logger logger = LoggerFactory.getLogger(HiveMetaWrapper.class);
    private static final String HIVE_META_DATA_URL = "http://berserker.bilibili.co/api/hive/meta/%s/%s";
    private static final String HIVE_DATA_URL = "http://berserker.bilibili.co/api/hive/data/%s/%s";
    private static final String HIVE_RECENT_PARTITION_READY_URL = "http://berserker.bilibili.co/keeper/partition/recentDataReadyPartition";
    private static final String HIVE_RECENT_PARTITION_URL = "http://berserker.bilibili.co/keeper/partition/recentPartition";
    private static final String SPLIT_INFO_STR = "^字段分隔符: ([\\s\\S]*?); 行分隔符: ([\\s\\S]*?)$";
    private static final Pattern SPLIT_INFO_PATTERN = Pattern.compile(SPLIT_INFO_STR);
    public static final Integer RECENT_HIVE_PARTITION_NUM = 5;

    private static final Integer KEEPER_FUNCTION_ERROR_CODE = -500;
    private static final Integer KEEPER_SERVER_ERROR_CODE = -200;


    public static Map<String, String> rewrite(Map<String, String> props, String databaseName, String tableName) throws Exception {
        HiveMetaDetail hiveMetaDetail = getMetaData(databaseName, tableName);
        props.put("location", hiveMetaDetail.getLocation());
        props.put("partitionKey", hiveMetaDetail.getPartitionKey());
        String splitInfo = hiveMetaDetail.getSplitInfo();
        Matcher matcher = SPLIT_INFO_PATTERN.matcher(splitInfo);
        if (matcher.find()) {
            props.put("fieldDelim", decodeEscape(matcher.group(1)));
            props.put("rowDelim", decodeEscape(matcher.group(2)));
        }
        props.put("owner", hiveMetaDetail.getOwner());
        //lancer bsql which contains traceId will not overwrite by hms,and so is the hmsOverwirte conf.
        boolean is_hms_overwrite = !(props.containsKey("traceId") || props.containsKey("hmsOverwrite"));
        if ("OrcSerde".equals(hiveMetaDetail.getStoreName())) {
            if (is_hms_overwrite) {
                props.put("format", HiveSideTableInfo.TABLE_FORMAT_ORC);
                if (hiveMetaDetail.getStates().containsKey("orc.compress")) {
                    props.put("compress", hiveMetaDetail.getStates().get("orc.compress"));
                }
            }
        } else if ("LazySimpleSerDe".equals(hiveMetaDetail.getStoreName())) {
            if (is_hms_overwrite) {
                props.put("format", HiveSideTableInfo.TABLE_FORMAT_TEXT);
            }
        } else if ("ParquetHiveSerDe".equals(hiveMetaDetail.getStoreName())) {
            props.put("format", HiveSideTableInfo.TABLE_FORMAT_PARQUET);
        } else {
            props.put("format", HiveSideTableInfo.TABLE_FORMAT_TEXT);
        }
        return props;

    }


    /**
     * Get the latest ready partition.
     *
     * @param tableInfo
     * @return
     * @throws Exception
     */
    public static HiveRecentPartitionReadyResp.Data.Partition getRecentReadyPartition(HiveSideTableInfo tableInfo) throws Exception {
        CloseableHttpClient httpClient = null;
        try {
            tableInfo.setDqcCheck(true);// The previous time was false,however the current time may be transfer to true
            httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost();
            httpPost.setURI(new URI(HIVE_RECENT_PARTITION_READY_URL));
            HiveMetaWrapper.HiveRecentPartitionReq req = HiveMetaWrapper.HiveRecentPartitionReq.builder()
                    .tableName(tableInfo.getTableName())
                    .partition(tableInfo.getPartitionKeys())
                    .build();
            StringEntity stringEntity = new StringEntity(JSON.toJSONString(req));
            httpPost.setEntity(stringEntity);
            HttpResponse dataHttpResponse = httpClient.execute(httpPost);
            String dataResponseString = EntityUtils.toString(dataHttpResponse.getEntity(), "UTF-8");
            HiveRecentPartitionReadyResp resp = JSON.parseObject(dataResponseString, HiveRecentPartitionReadyResp.class);
            if (resp.code == KEEPER_SERVER_ERROR_CODE) {
                throw new Exception(resp.msg);
            }

            if (resp.code == KEEPER_FUNCTION_ERROR_CODE) {
                tableInfo.setDqcCheck(false);
                return null;
            }
            HiveRecentPartitionReadyResp.Data.Partition partition = resp.getData().getPartition();
            if (partition.getLog_date() == null) {
                logger.error("维表:{}符合数据质量校验条件，但当前无可用分区", req.tableName);
                return null;
            }
            return partition;
        } catch (Exception e) {
            throw new Exception("Call for Keeper server getRecentReadyPartition encounter exception", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }


    /**
     * Get the latest partitions,data quality is not guaranteed.
     *
     * @param tableInfo
     * @return
     * @throws Exception
     */
    public static List<HiveRecentPartitionReadyResp.Data.Partition> getRecentPartitions(HiveSideTableInfo tableInfo) throws Exception {
        CloseableHttpClient httpClient = null;
        try {
            HiveMetaWrapper.HiveRecentPartitionReq req = HiveRecentPartitionReq.builder()
                    .tableName(tableInfo.getTableName())
                    .partition(tableInfo.getPartitionKeys())
                    .limit(RECENT_HIVE_PARTITION_NUM)
                    .build();
            httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost();
            httpPost.setURI(new URI(HIVE_RECENT_PARTITION_URL));
            StringEntity stringEntity = new StringEntity(JSON.toJSONString(req));
            httpPost.setEntity(stringEntity);
            HttpResponse dataHttpResponse = httpClient.execute(httpPost);
            String dataResponseString = EntityUtils.toString(dataHttpResponse.getEntity(), "UTF-8");
            HiveRecentPartitionsResp resp = JSON.parseObject(dataResponseString, HiveRecentPartitionsResp.class);
            if (resp.code != 200) {
                throw new Exception(resp.msg);
            }
            return resp.data.getPartitions();
        } catch (Exception e) {
            throw new Exception("Call for Keeper server getRecentReadyPartition encounter exception", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }


    /**
     * Get the lastest exist partition.
     * The data quality cannot be guaranteed, only the existence of the partition and the existence of the path are guaranteed.
     *
     * @param db,table
     * @return
     * @throws Exception
     */
	/*public static HiveRecentPartitionResp.DataBean.PartitionBean getRecentPartition(HiveRecentPartitionReq req) throws Exception{

	}*/
    public static HiveMetaDetail getMetaData(String db, String table) throws Exception {
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet();
            httpGet.setURI(new URI(String.format(HIVE_META_DATA_URL, db, table)));
            HttpResponse httpResponse = httpClient.execute(httpGet);
            String responseString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            HiveMetaDetail hiveMetaDetail = JSON.parseObject(responseString, HiveMetaDetail.class);
            if (hiveMetaDetail.location == null) {
                throw new RuntimeException("找不到hive表:" + table + " 信息,请确认表已经创建");
            }
            httpGet = new HttpGet();
            httpGet.setURI(new URI(String.format(HIVE_DATA_URL, db, table)));
            HttpResponse dataHttpResponse = httpClient.execute(httpGet);
            String dataResponseString = EntityUtils.toString(dataHttpResponse.getEntity(), "UTF-8");
            HiveDataDetail hiveDataDetail = JSON.parseObject(dataResponseString, HiveDataDetail.class);
            hiveMetaDetail.setStoreName(hiveDataDetail.getStoreName());
            return hiveMetaDetail;
        } catch (Exception e) {
            logger.error("Get hive meta data error", e);
            throw e;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * get the ready partition location path
     *
     * @param tableInfo
     * @return
     * @throws Exception
     */
    public static String getReadyPath(HiveSideTableInfo tableInfo) throws Exception {
        HiveRecentPartitionReadyResp.Data.Partition partition = getRecentReadyPartition(tableInfo);
        if (partition == null) {
            return null;
        }
        return buildPath(tableInfo, partition);
    }


    /**
     * build the the partition location path.
     *
     * @param tableInfo
     * @param partition
     * @return
     */
    public static String buildPath(HiveSideTableInfo tableInfo, HiveRecentPartitionReadyResp.Data.Partition partition) {
        String location = tableInfo.getLocation();
        if (StringUtils.isBlank(location)) {
            throw new RuntimeException(tableInfo.getTableName() + " location is empty");
        }
        StringBuilder hdfsPathBuilder = new StringBuilder(location);
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        for (int i = 0; i < partitionKeys.size(); i++) {
            hdfsPathBuilder.append("/").append(partitionKeys.get(i)).append("=");
            if (i == 0) {
                hdfsPathBuilder.append(partition.getLog_date());
            } else {
                hdfsPathBuilder.append(partition.getLog_hour());
            }
        }
        return hdfsPathBuilder.toString();
    }


    /**
     * Check whether the path meets the load condition.
     *
     * @param fs
     * @param remotePath
     * @throws IOException
     */
    public static void validPath(FileSystem fs, String remotePath, boolean checkSuccessTag) throws IOException {
        if (!fs.exists(new Path(remotePath))) {
            throw new RuntimeException(remotePath + " not exists.");
        }

        if (checkSuccessTag) {
            boolean successTag = fs.exists(new Path(remotePath, "_SUCCESS"))
                    || fs.exists(new Path(remotePath, ".SUCCESS"));
            if (!successTag) {
                throw new RuntimeException(remotePath + " SUCCESS file not Exist.");
            }
        }
    }

    @lombok.Data
    public static class HiveMetaDetail {

        private String database;
        private String tableType;
        private String table;
        private String location;
        private String partitionKey;
        private String latestPartition;
        private String earliestPartition;
        private Map<String, String> states;
        private List<HiveMetaColumn> columns;
        private String splitInfo;
        private String owner;
        private String storeName;
    }

    @lombok.Data
    public static class HiveDataDetail {

        private String storeName; // hive表存储格式
    }

    @lombok.Data
    public static class HiveMetaColumn {
        private int index;
        private String column;
        private String columnType;
        private String columnDesc;
    }

    /**
     * Request body for recent hive ready partition
     */
    @lombok.Data
    @Builder
    public static class HiveRecentPartitionReq {
        private String tableName;
        private List<String> partition;
        private Integer limit;
    }

    /**
     * Response body for recent hive ready partition
     */
    @lombok.Data
    public static class HiveRecentPartitionReadyResp {
        private int code;
        private Data data;
        private String msg;

        @lombok.Data
        public static class Data {
            private Partition partition;

            @lombok.Data
            public static class Partition {
                private String log_date;
                private String log_hour;
            }
        }
    }

    @lombok.Data
    public static class HiveRecentPartitionsResp {
        private int code;
        private Data data;
        private String msg;

        @lombok.Data
        public static class Data {
            private List<HiveRecentPartitionReadyResp.Data.Partition> partitions;

        }
    }


    /**
     * 将前端展示的分隔符转换成程序中的分隔符
     *
     * @param s
     * @return
     */
    private static String decodeEscape(String s) {
        return s.replaceAll("\\\\t", "\t")
                .replaceAll("\\\\r", "\r")
                .replaceAll("\\\\n", "\n")
                .replaceAll("\\\\u0001", "\u0001");
    }


}
