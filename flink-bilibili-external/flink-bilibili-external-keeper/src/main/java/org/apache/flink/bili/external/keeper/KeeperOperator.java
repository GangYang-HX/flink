package org.apache.flink.bili.external.keeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.bili.external.keeper.dto.resp.KeeperGetTableInfoResp;

/**
 * @Author: JinZhengyu
 * @Date: 2022/8/16 下午2:47
 */
@Slf4j
public class KeeperOperator {

	private static final String KAFKA_DS_SYMBOL = "Kafka";
	private static final String KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrapServers";

	/**
	 * get kafka bootstrapServers
	 *
	 * @param topic
	 * @return
	 */
	public static String getKafkaBrokerInfo(String topic) {
		String bootstrapServers = "";
		try {
			KeeperMessage<KeeperGetTableInfoResp> tableInfo = KeeperHandler.getTableInfo(KAFKA_DS_SYMBOL, null, topic);
			boolean successful = KeeperMessage.isSuccessful(tableInfo);
			if (successful) {
				bootstrapServers = tableInfo.getData().getDatabase().getProperties().getOrDefault(KAFKA_BOOTSTRAP_SERVERS_KEY, "");
			} else {
				String errorMsg = KeeperMessage.errorMsg(tableInfo);
				log.error("get topic:{} bootstrapServers info error,errorMsg:{},exceptionMsg:{}", topic, errorMsg, tableInfo.getMessage());
			}

		} catch (Exception e) {
			log.error("get topic:{} bootstrapServers info encounter exception", topic, e);
		}
		log.info("get topic:{}, bootstrapServers:{}", topic, bootstrapServers);
		return bootstrapServers;
	}
}
