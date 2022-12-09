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
public class HiveTableUtil {

	/**
	 * 构造表属性
	 *
	 * @param tableDto
	 * @return
	 */
	public static Map<String, String> constructTablePropsMap(TableDto tableDto) {
		Map<String, String> flinkPropsMap = new HashMap<>();
		flinkPropsMap.put("connector.type", "bsql-hive");
		flinkPropsMap.put("tableName", getHiveTableName(tableDto));
		flinkPropsMap.put("format", getHiveFormat(tableDto));
		flinkPropsMap.put("compress", getHiveOrcCompress(tableDto));
		flinkPropsMap.put("partitionKey", getHivePartitionKey(tableDto));
		flinkPropsMap.put("location", getHiveLocation(tableDto));
		flinkPropsMap.put("fieldDelim", getHiveFieldDelim(tableDto));
		flinkPropsMap.put("rowDelim", getHiveLineDelim(tableDto));

		return flinkPropsMap;
	}

	/**
	 * get hive format
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHiveFormat(TableDto tableDto) {
		Map<String, String> props = tableDto.getPropertiesMap();
		String inputFormat = props.get("inputFormat");
		if (StringUtils.isEmpty(inputFormat)) {
			throw new CatalogException(
				String.format("table %s.%s format property not exist", tableDto.getDatabase().getDatabaseName(), tableDto.getTableName()));
		}

		if (inputFormat.toLowerCase().contains("orc")) {
			return "orc";
		} else if (inputFormat.toLowerCase().contains("parquet")) {
			return "parquet";
		} else {
			return "text";
		}
	}

	/**
	 * get hive table name
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHiveTableName(TableDto tableDto) {
		return String.format("%s.%s", tableDto.getDatabase().getDatabaseName(), tableDto.getTableName());
	}

	/**
	 * get hive partition key
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHivePartitionKey(TableDto tableDto) {
		Object[] partitions = tableDto.getColumnsList().stream().filter(ColumnDto::getIsPartition).map(ColumnDto::getColName).toArray();
		if (partitions.length < 1) {
			throw new CatalogException(
				String.format("table %s.%s partition not exist", tableDto.getDatabase().getDatabaseName(), tableDto.getTableName()));
		}
		return StringUtils.join(partitions, ",");
	}

	/**
	 * get hive location
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHiveLocation(TableDto tableDto) {
		return tableDto.getPropertiesMap().get("location");
	}

	/**
	 * get hive orc compress
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHiveOrcCompress(TableDto tableDto) {
		return StringUtils.defaultString(tableDto.getPropertiesMap().get("orc.compress"));
	}

	/**
	 * get hive line delim
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHiveLineDelim(TableDto tableDto) {
		return StringUtils.defaultString(tableDto.getPropertiesMap().get("line.delim"));
	}

	/**
	 * get hive field delim
	 *
	 * @param tableDto
	 * @return
	 */
	private static String getHiveFieldDelim(TableDto tableDto) {
		return StringUtils.defaultString(tableDto.getPropertiesMap().get("field.delim"));
	}
}
