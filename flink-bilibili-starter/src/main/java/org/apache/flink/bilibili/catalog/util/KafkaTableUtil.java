package org.apache.flink.bilibili.catalog.util;

import com.bapis.datacenter.service.keeper.ColumnDto;
import com.bapis.datacenter.service.keeper.TableDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author haozhugogo
 */
public class KafkaTableUtil {

	/**
	 * 构造表属性
	 *
	 * @param tableDto
	 * @return
	 */
	public static Map<String, String> constructTablePropsMap(TableDto tableDto) {

		Map<String, String> flinkPropsMap = new HashMap<>();

		flinkPropsMap.put("connector", "bsql-kafka10");
		flinkPropsMap.put("bootstrapServers", tableDto.getDatabase().getPropertiesMap().get("bootstrapServers"));
		flinkPropsMap.put("topic", getTopic(tableDto));

		return flinkPropsMap;
	}

	/**
	 * get topic
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getTopic(TableDto tableDto) {
		return StringUtils.defaultString(tableDto.getTableName());
	}

	/**
	 * get bootstrap
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getBootstrap(TableDto tableDto) {
		return tableDto.getDatabase().getPropertiesMap().get("bootstrapServers");
	}
}
