package org.apache.flink.connectors.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.table.PartitionFetcher;
import org.apache.flink.table.catalog.ObjectPath;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Fetcher to fetch the suitable partitions of a keeper table.
 */
@Internal
public class KeepReadPartitionFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(KeepReadPartitionFetcher.class);

    private static final String HIVE_RECENT_PARTITION_READY_URL = "http://berserker.bilibili.co/keeper/partition/recentDataReadyPartition";

    private static final Integer KEEPER_FUNCTION_ERROR_CODE = -500;
    private static final Integer KEEPER_SERVER_ERROR_CODE = -200;

    /**
     * When there is a ready partition, use the ready partition, otherwise use the latest partition.
     * @throws Exception
     */
    public static  <P extends HiveTablePartition> List<P> getRecentReadyPartition(
            ObjectPath tableFullPath, PartitionFetcher.Context<P> fetcherContext, List<P> partValueList) throws Exception {
        List<String> readyPartitionValues = getRecentReadyPartition(partValueList);
        if (CollectionUtils.isEmpty(readyPartitionValues)) {
            return partValueList;
        }
        final ObjectMapper objectMapper = new ObjectMapper();
        Optional<P> readyHiveTablePartitionOptional = fetcherContext.getPartition(
                readyPartitionValues);
        if (!readyHiveTablePartitionOptional.isPresent()) {
            throw new IllegalArgumentException(String.format(
                    "dqc ready partition does not exist,table=%s,partition=%s", tableFullPath, objectMapper.writeValueAsString(readyPartitionValues)));
        }
        return Collections.singletonList(readyHiveTablePartitionOptional.get());
    }

    /**
     * Get the latest ready partition values.
     *
     * @return
     *
     * @throws Exception
     */
    public static  <P extends HiveTablePartition> List<String> getRecentReadyPartition(List<P> partitions) throws Exception {
        CloseableHttpClient httpClient = null;
        try {
            if (CollectionUtils.isEmpty(partitions)) {
                return new ArrayList<>();
            }
            httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost();
            httpPost.setURI(new URI(HIVE_RECENT_PARTITION_READY_URL));

            final ObjectMapper objectMapper = new ObjectMapper();
            HiveRecentPartitionReq req = createPartitionReq(partitions);
            StringEntity stringEntity = new StringEntity(objectMapper.writeValueAsString(req));
            LOG.info("Hive dimension table request keeper, parameters : {}", objectMapper.writeValueAsString(req));
            httpPost.setEntity(stringEntity);
            HttpResponse dataHttpResponse = httpClient.execute(httpPost);

            String dataResponseString = EntityUtils.toString(dataHttpResponse.getEntity(), "UTF-8");
            HiveRecentPartitionReadyResp resp = objectMapper.readValue(
                    dataResponseString,
                    HiveRecentPartitionReadyResp.class);

            if (resp.code == KEEPER_SERVER_ERROR_CODE) {
                throw new RuntimeException(resp.msg);
            }

            if (resp.code == KEEPER_FUNCTION_ERROR_CODE) {
                LOG.warn("Hive dimension table request keeper is failed,{}", resp.getMsg());
                return new ArrayList<>();
            }
            HiveRecentPartitionReadyResp.Data.PartitionValue partition = resp.getData().getPartition();
            if (partition.getLogDate() == null) {
                LOG.error("Hive dimension table: {} meets the data quality check conditions, but currently has no available partitions", req.tableName);
                return new ArrayList<>();
            }
            LOG.info("When the Hive dimension table joins, the ready partition,table={},partitionDesc={}",
                    req.getTableName(),
                    objectMapper.writeValueAsString(partition));
            List<String> partitionValues = new ArrayList<>();
            partitionValues.add(partition.getLogDate());
            partitionValues.add(partition.getLogHour());
            return partitionValues;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Call for Keeper server getRecentReadyPartition encounter exception", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    private static  <P extends HiveTablePartition> HiveRecentPartitionReq createPartitionReq(List<P> partitions) {
        HiveRecentPartitionReq req = new HiveRecentPartitionReq();
        P partition = partitions.get(0);
        String tableName = partition.getTableProps().getProperty("name");
        List<String> partitionKeys = new ArrayList<>(partition.getPartitionSpec().keySet());
        LOG.info(
                "When the Hive dimension table joins, the latest partition,table={},partitionDesc={}",
                tableName,
                partition.getPartitionSpec());
        req.setTableName(tableName);
        req.setPartition(partitionKeys);
        return req;
    }

    static class HiveRecentPartitionReq {
        private String tableName;
        private List<String> partition;
        private Integer limit;

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setPartition(List<String> partition) {
            this.partition = partition;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }

        public Integer getLimit() {
            return limit;
        }

        public List<String> getPartition() {
            return partition;
        }

        public String getTableName() {
            return tableName;
        }
    }

    static class HiveRecentPartitionReadyResp {
        private int code;
        private Data data;
        private String msg;

        public static class Data {
            private PartitionValue partition;

            public static class PartitionValue {

                @JsonProperty("log_date")
                private String logDate;

                @JsonProperty("log_hour")
                private String logHour;

                public String getLogDate() {
                    return logDate;
                }

                public void setLogDate(String logDate) {
                    this.logDate = logDate;
                }

                public String getLogHour() {
                    return logHour;
                }

                public void setLogHour(String logHour) {
                    this.logHour = logHour;
                }
            }

            public PartitionValue getPartition() {
                return partition;
            }

            public void setPartition(PartitionValue partition) {
                this.partition = partition;
            }
        }

        public void setCode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public void setData(Data data) {
            this.data = data;
        }

        public Data getData() {
            return data;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }
    }
}
