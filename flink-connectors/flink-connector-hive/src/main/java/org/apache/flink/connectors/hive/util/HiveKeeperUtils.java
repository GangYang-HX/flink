package org.apache.flink.connectors.hive.util;

import org.apache.flink.connectors.hive.HiveTablePartition;

import com.google.gson.Gson;
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
import java.util.List;

/**
 * 用于和Keeper进行交互，获取真正有效的ready分区
 */
public class HiveKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveKeeperUtils.class);

    private static final String HIVE_RECENT_PARTITION_READY_URL = "http://berserker.bilibili.co/keeper/partition/recentDataReadyPartition";

    private static final Integer KEEPER_FUNCTION_ERROR_CODE = -500;
    private static final Integer KEEPER_SERVER_ERROR_CODE = -200;

    /**
     * Get the latest ready partition.
     *
     * @return
     *
     * @throws Exception
     */
    public static List<String> getRecentReadyPartition(List<HiveTablePartition> partitions) throws Exception {
        CloseableHttpClient httpClient = null;
        try {
            if (CollectionUtils.isEmpty(partitions)) {
                return new ArrayList<>();
            }
            HiveRecentPartitionReq req = createPartitionReq(partitions);

            httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost();
            httpPost.setURI(new URI(HIVE_RECENT_PARTITION_READY_URL));

            Gson gson = new Gson();
            StringEntity stringEntity = new StringEntity(gson.toJson(req));
            httpPost.setEntity(stringEntity);
            HttpResponse dataHttpResponse = httpClient.execute(httpPost);

            String dataResponseString = EntityUtils.toString(dataHttpResponse.getEntity(), "UTF-8");
            HiveRecentPartitionReadyResp resp = gson.fromJson(
                    dataResponseString,
                    HiveRecentPartitionReadyResp.class);

            if (resp.code == KEEPER_SERVER_ERROR_CODE) {
                throw new RuntimeException(resp.msg);
            }

            if (resp.code == KEEPER_FUNCTION_ERROR_CODE) {
                LOG.warn("Hive dimension table request keeper is failed,{}", resp.getMsg());
                return new ArrayList<>();
            }
            HiveRecentPartitionReadyResp.Data.Partition partition = resp.getData().getPartition();
            if (partition.getLog_date() == null) {
                LOG.error(
                        "Dimension table: {} meets the data quality check conditions, but currently has no available partitions",
                        req.tableName);
                return new ArrayList<>();
            }
            List<String> partitionValues = new ArrayList<>();
            partitionValues.add(partition.getLog_date());
            partitionValues.add(partition.getLog_hour());

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

    /**
     * TODO : Check whether the ready partition is consistent with the latest partition
     *
     * @param partition
     * @param readyPartition
     *
     * @return
     */
    private static boolean checkReadyPartitionSame(
            HiveTablePartition partition,
            HiveRecentPartitionReadyResp.Data.Partition readyPartition) {
        return true;
    }

    private static HiveRecentPartitionReq createPartitionReq(List<HiveTablePartition> partitions) {
        HiveRecentPartitionReq req = new HiveRecentPartitionReq();
        HiveTablePartition partition = partitions.get(0);
        String tableName = partition.getTableProps().getProperty("name");
        List<String> partitionKeys = new ArrayList<>(partition.getPartitionSpec().keySet());
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

    public static class HiveRecentPartitionReadyResp {
        private int code;
        private Data data;
        private String msg;

        public static class Data {
            private Partition partition;

            public static class Partition {
                private String log_date;
                private String log_hour;

                public void setLog_date(String log_date) {
                    this.log_date = log_date;
                }

                public String getLog_date() {
                    return log_date;
                }

                public void setLog_hour(String log_hour) {
                    this.log_hour = log_hour;
                }

                public String getLog_hour() {
                    return log_hour;
                }
            }

            public void setPartition(Partition partition) {
                this.partition = partition;
            }

            public Partition getPartition() {
                return partition;
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
