package org.apache.flink.bili.external.keeper;

import org.apache.flink.bili.external.keeper.dto.resp.KeeperGetTableInfoResp;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static org.apache.flink.bili.external.keeper.constant.HiveConstants.HIVE_DS_SYMBOL;
import static org.apache.flink.bili.external.keeper.constant.HiveConstants.HIVE_TABLE_LOCATION_SYMBOL;
import static org.apache.flink.bili.external.keeper.constant.KafkaConstants.KAFKA_BOOTSTRAP_SERVERS_KEY;
import static org.apache.flink.bili.external.keeper.constant.KafkaConstants.KAFKA_DS_SYMBOL;

/** Called by other flink modules. */
@Slf4j
public class KeeperOperator {
    /**
     * get kafka bootstrapServers
     *
     * @param topic
     * @return
     */
    public static String getKafkaBrokerInfo(String topic) {
        String bootstrapServers = "";
        try {
            KeeperMessage<KeeperGetTableInfoResp> tableInfo =
                    KeeperHandler.getTableInfo(KAFKA_DS_SYMBOL, null, topic);
            boolean successful = KeeperMessage.isSuccessful(tableInfo);
            if (successful) {
                bootstrapServers =
                        tableInfo
                                .getData()
                                .getDatabase()
                                .getProperties()
                                .getOrDefault(KAFKA_BOOTSTRAP_SERVERS_KEY, "");
            } else {
                String errorMsg = KeeperMessage.errorMsg(tableInfo);
                log.error(
                        "get topic:{} bootstrapServers info error,errorMsg:{},exceptionMsg:{}",
                        topic,
                        errorMsg,
                        tableInfo.getMessage());
            }

        } catch (Exception e) {
            log.error("get topic:{} bootstrapServers info encounter exception", topic, e);
        }
        log.info("get topic:{}, bootstrapServers:{}", topic, bootstrapServers);
        return bootstrapServers;
    }

    /**
     * get hive table column list
     *
     * @param databaseName {@link String}
     * @param tableName {@link String}
     * @return {@link List}<{@link KeeperGetTableInfoResp.Column}>
     */
    public static List<KeeperGetTableInfoResp.Column> getTableColumnList(
            String databaseName, String tableName) throws Exception {
        return getTableInfoData(databaseName, tableName).getColumns();
    }

    /**
     * get hive table location
     *
     * @param databaseName {@link String}
     * @param tableName {@link String}
     * @return {@link String}
     */
    public static String getTableLocation(String databaseName, String tableName) throws Exception {
        return getTableInfoData(databaseName, tableName)
                .getProperties()
                .getOrDefault(HIVE_TABLE_LOCATION_SYMBOL, "");
    }

    /**
     * get hive table info data
     *
     * @param databaseName {@link String}
     * @param tableName {@link String}
     * @return {@link KeeperGetTableInfoResp}
     */
    private static KeeperGetTableInfoResp getTableInfoData(String databaseName, String tableName)
            throws Exception {
        KeeperMessage<KeeperGetTableInfoResp> tableInfo =
                KeeperHandler.getTableInfo(HIVE_DS_SYMBOL, databaseName, tableName);
        boolean successful = KeeperMessage.isSuccessful(tableInfo);
        if (successful) {
            log.info("{}.{} get table info data", databaseName, tableName);
            return tableInfo.getData();
        } else {
            String errorMsg = KeeperMessage.errorMsg(tableInfo);
            throw new Exception(errorMsg);
        }
    }
}
