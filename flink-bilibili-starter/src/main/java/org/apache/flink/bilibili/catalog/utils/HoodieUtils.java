package org.apache.flink.bilibili.catalog.utils;

import com.bapis.datacenter.service.keeper.ColumnDto;
import com.bapis.datacenter.service.keeper.TableDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Shin
 * @version 2022/8/10 14:26
 */

public class HoodieUtils {

    private static final String HUDI_PARQUET_RO = "hudi_parquet";
    private static final String HUDI_PARQUET_RT = "hudi_parquet_rt";

    public static List<ColumnDto> schemaFilter(TableDto tableDto) {
        List<ColumnDto> columnDtos = tableDto.getColumnsList();
        return columnDtos
                .stream()
                .filter(columnDto -> !columnDto.getColName().startsWith("_hoodie_"))
                .collect(Collectors.toList());
    }

    public static boolean isHoodieTable(TableDto tableDto) {
        if (tableDto.getPropertiesMap().get("hiveStoreType") != null) {
            String hiveStoreType = tableDto.getPropertiesMap().get("hiveStoreType");
            return isRoTable(hiveStoreType) || isRtTable(hiveStoreType);
        } else {
            throw new CatalogException("Can't get hiveStoreType from keeper");
        }

    }

    private static boolean isRoTable(String hiveStoreType) {
        return StringUtils.equalsIgnoreCase(hiveStoreType, HUDI_PARQUET_RO);
    }

    private static boolean isRtTable(String hiveStoreType) {
        return StringUtils.equalsIgnoreCase(hiveStoreType, HUDI_PARQUET_RT);
    }
}
