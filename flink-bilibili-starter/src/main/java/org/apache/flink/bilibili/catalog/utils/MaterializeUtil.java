package org.apache.flink.bilibili.catalog.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalogConfig;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateMaterializedViewOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author zhangyang
 * @Date:2022/6/6
 * @Time:11:00
 */
public class MaterializeUtil {

    private final static String INSERT_INTO_FORMAT = "insert into %s %s";
    private final static String CREATE_HUDI_TABLE_FORMAT = "create table %s %s PRIMARY KEY(%s) with(%s)";
    private final static Map<String, String> DEFAULT_HUDI_PROPS = new HashMap<>();
    private final static String HOODIE_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    private final static String PRIMARY_KEY = "primary_key";
    private final static Logger LOG = LoggerFactory.getLogger(MaterializeUtil.class);


    static {
        DEFAULT_HUDI_PROPS.put("connector", "hudi");
        DEFAULT_HUDI_PROPS.put("write.operation", "upsert");
        DEFAULT_HUDI_PROPS.put("table.type", "MERGE_ON_READ");
    }

    public static void convert(boolean explain,StreamExecutionEnvironment env, StreamTableEnvironment tabEnv, StatementSet statementSet, String sql) {
        Parser parser = ((TableEnvironmentImpl) tabEnv).getParser();
        List<Operation> operations = parser.parse(sql);
        CreateMaterializedViewOperation mvOperation = (CreateMaterializedViewOperation) operations.get(0);

        ObjectIdentifier oldIdentifier = mvOperation.getViewIdentifier();
        ObjectIdentifier newIdentifier = ObjectIdentifier.of(oldIdentifier.getCatalogName(),
                oldIdentifier.getDatabaseName(),
                oldIdentifier.getObjectName() + "_hoodie_mv");
        String tableName = newIdentifier.toString();
        String querySql = mvOperation.getCatalogMaterializedView().getOriginalQuery();
        String insertSql = String.format(INSERT_INTO_FORMAT, tableName, querySql);

        Optional<ContextResolvedTable> table = ((TableEnvironmentImpl) tabEnv).getCatalogManager().
                getTable(mvOperation.getViewIdentifier());
        Map<String, String> options = table.get().getTable().getOptions();

        String schemaStr = table.get().getResolvedSchema().toString();

        String primaryKeyStr = mvOperation.getCatalogMaterializedView().getOptions().get(PRIMARY_KEY);
        if (StringUtils.isEmpty(primaryKeyStr)) {
            throw new UnsupportedOperationException("primary key is null");
        }

        String withStr = "";
        String connector = options.get(FactoryUtil.CONNECTOR.key());
        if (connector.equalsIgnoreCase(SqlCreateHiveTable.IDENTIFIER)) {
            String inputFormat = options.get(HiveCatalogConfig.INPUT_FORMAT);
            if (!inputFormat.equalsIgnoreCase(HOODIE_FORMAT)) {
                throw new UnsupportedOperationException("materialize only support hudi table");
            }
            Map<String, String> tableProps = new HashMap<>(DEFAULT_HUDI_PROPS);
            tableProps.put("path", options.get(HiveCatalogConfig.LOCATION));
            List<String> withList = new ArrayList<>(tableProps.size());
            for (String key : tableProps.keySet()) {
                withList.add(String.format("\"%s\" = \"%s\"", key, tableProps.get(key)));
            }
            withStr = StringUtils.join(withList, ",");
        }

        String createHudiTable = String.format(CREATE_HUDI_TABLE_FORMAT, tableName, schemaStr, primaryKeyStr, withStr);
        LOG.info("createHudiTable:{}", createHudiTable);
        LOG.info("insertSql:{}", insertSql);
        if(!explain) {
            tabEnv.executeSql(createHudiTable);
        }
        statementSet.addInsertSql(insertSql);

    }
}
