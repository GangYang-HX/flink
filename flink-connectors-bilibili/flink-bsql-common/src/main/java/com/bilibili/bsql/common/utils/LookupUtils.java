package com.bilibili.bsql.common.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.types.DataType;

import org.apache.commons.compress.utils.Lists;

import java.util.List;

/** LookupUtils. */
public class LookupUtils {

    public static final String COMMA = ",";

    public static String getSqlCondition(
            int[][] keys, String tableName, String[] fieldName, DataType[] fieldTypes) {
        String sqlCondition = "select ${selectField} from ${tableName} where ";
        // get join on field <fieldName, fieldType>
        List<Tuple3<String, DataType, String>> joinOnFields = Lists.newArrayList();

        for (int[] key : keys) {
            String name = fieldName[key[0]];
            DataType dataType = fieldTypes[key[0]];
            joinOnFields.add(new Tuple3<>(name, dataType, null));
        }

        for (int i = 0; i < joinOnFields.size(); i++) {
            String equalField = joinOnFields.get(i).f0;
            sqlCondition += equalField + "=?";
            if (i != joinOnFields.size() - 1) {
                sqlCondition += " and ";
            }
        }

        sqlCondition =
                sqlCondition
                        .replace("${tableName}", tableName)
                        .replace("${selectField}", String.join(COMMA, fieldName));

        return sqlCondition;
    }
}
