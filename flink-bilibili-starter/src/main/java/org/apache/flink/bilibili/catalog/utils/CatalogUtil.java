package org.apache.flink.bilibili.catalog.utils;

import com.bapis.datacenter.service.keeper.ColumnDto;
import com.bapis.datacenter.service.keeper.TableDto;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author : luotianran
 * @version V1.0
 * @Description:
 * @date Date : 2022年04月24日
 */
public class CatalogUtil {
    private final static Logger LOG = LoggerFactory.getLogger(CatalogUtil.class);

    /**
     * 构造表属性
     *
     * @param tableDto
     * @return
     */
    public static Map<String, String> constructHiveTablePropsMap(TableDto tableDto) {
        Map<String, String> flinkPropsMap = new HashMap<>();
        flinkPropsMap.put("connector.type", "bsql-hive");
        flinkPropsMap.put("tableName", getHiveTableName(tableDto));
        flinkPropsMap.put("format", getHiveFormat(tableDto));
        flinkPropsMap.put("compress", getHiveOrcCompress(tableDto));
        flinkPropsMap.put("partitionKey", getHivePartitionKey(tableDto));
        return flinkPropsMap;
    }


    private static String getHiveDb(TableDto tableDto) {
        return tableDto.getDatabase().getDatabaseName();
    }

    private static String getHiveTable(TableDto tableDto) {
        return tableDto.getTableName();
    }
    public static String getPrimaryKey(TableDto tableDto) {
        Object[] pkList = tableDto.getColumnsList().stream().filter(ColumnDto::getIsPrimary).map(ColumnDto::getColName).toArray();
//        if (pkList.length < 1) {
//            throw new CatalogException(
//                    String.format("table %s.%s primary key not exist", tableDto.getDatabase().getDatabaseName(), tableDto.getTableName()));
//        }
        return StringUtils.join(pkList, ",");
    }

    /**
     * get hive table primary key as array
     * @param tableDto
     *
     * @return {@link String[]}
     */
    public static String[] getPrimaryKeyArray(TableDto tableDto) {
        String[] pkList = tableDto
                .getColumnsList()
                .stream()
                .filter(ColumnDto::getIsPrimary)
                .map(ColumnDto::getColName)
                .toArray(String[]::new);

//        if (pkList.length < 1) {
//            throw new CatalogException(
//                    String.format("table %s.%s primary key not exist", tableDto.getDatabase().getDatabaseName(), tableDto.getTableName()));
//        }
        return pkList;
    }

    /**
     * get hive table location
     * @param tableDto
     *
     * @return {@link String}
     */
    private static String getHiveLocation(TableDto tableDto) {
        if (tableDto.getPropertiesMap().get("location") != null) {
            return tableDto.getPropertiesMap().get("location");
        } else {
            throw new CatalogException(
                    String.format("table %s.%s location not exist", tableDto.getDatabase().getDatabaseName(), tableDto.getTableName()));
        }
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
     * get hive partition key as List
     * @param tableDto
     *
     * @return {@link List}<{@link String}>
     */
    public static List<String> getHivePartitionKeyList(TableDto tableDto) {
        List<String> partitions = tableDto.getColumnsList().stream().filter(ColumnDto::getIsPartition).map(ColumnDto::getColName).collect(Collectors.toList());
        if (partitions.size() < 1) {
            return Collections.emptyList();
        }
        return partitions;
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
    
    public static void registerHoodieCatalog(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "bili-hudi");
        try {
            Catalog biliHoodieCatalog = FactoryUtil.createCatalog(
                    "bili-hudi",
                    options,
                    env.getConfiguration(),
                    Thread.currentThread()
                            .getContextClassLoader());
            if (biliHoodieCatalog != null) {
                tabEnv.registerCatalog("Hudi", biliHoodieCatalog);
                LOG.info("register catalog bili-hudi success.");
            }
        } catch (Exception e) {
            LOG.info("register catalog bili-hudi error.");
        }

    }
}
