package com.bilibili.bsql.rdb.table;

import com.bilibili.bsql.common.types.ClickFloatArray;
import com.bilibili.bsql.common.types.ClickIntArray;
import com.bilibili.bsql.common.types.ClickStringArray;
import com.bilibili.bsql.rdb.format.RetractJDBCOutputFormat;
import com.bilibili.bsql.rdb.tableinfo.RdbSinkTableInfo;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlRdbDynamicSink.java
 * @description This is the description of BsqlRdbDynamicSink.java
 * @createTime 2020-10-20 15:58:00
 */
public abstract class BsqlRdbDynamicSinkBase implements DynamicTableSink, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(BsqlRdbDynamicSinkBase.class);

    private final RdbSinkTableInfo sinkTableInfo;
    protected String driverName;
    protected String dbURL;
    protected String userName;
    protected String password;
    protected String dbType;
    protected int[] sqlTypes;
    protected String tableName;
    protected String sql;
    protected String tps;
    protected int insertType = 0;
    protected String realTableNames;
    protected String targetTables;
    protected String targetDBs;
    @Deprecated
    protected Map ddlUDFMap;
    protected boolean autoCommit = true;
    protected Integer batchMaxTimeout;
    protected Integer batchMaxCount = 1;
    protected String[] fieldNames;
	protected Integer maxRetries;

    protected List<String> primaryKeys;

    public static final int CLICK_INT_ARRAY = 3001;
    public static final int CLICK_FLOAT_ARRAY = 3002;
    public static final int CLICK_STRING_ARRAY = 3003;

    public BsqlRdbDynamicSinkBase(RdbSinkTableInfo sinkTableInfo) {
        this.sinkTableInfo = sinkTableInfo;
    }

    /**
     * 对应于 RdbSink的genStreamSink方法
     *
     * @param sinkTableInfo
     */
    private void initParams(RdbSinkTableInfo sinkTableInfo) {
        this.batchMaxTimeout = sinkTableInfo.getBatchMaxTimeout() != null ? sinkTableInfo.getBatchMaxTimeout() : batchMaxTimeout;
        this.batchMaxCount = sinkTableInfo.getBatchMaxCount() != null ? sinkTableInfo.getBatchMaxCount() : batchMaxCount;

        this.dbURL = sinkTableInfo.getDbURL();
        this.userName = sinkTableInfo.getUserName();
        this.password = sinkTableInfo.getPassword();
        this.tps = sinkTableInfo.getTps();
        this.tableName = sinkTableInfo.getTableName();

        List<String> fields = Arrays.asList(sinkTableInfo.getFieldNames());
        List<LogicalType> fieldTypeArray = Arrays.asList(sinkTableInfo.getFieldLogicalType());

        this.driverName = getDriverName();
        this.primaryKeys = sinkTableInfo.getPrimaryKeys();
        this.dbType = sinkTableInfo.getType();

        if (null != sinkTableInfo.getInsertType()) {
            this.insertType = sinkTableInfo.getInsertType();
        }

        this.realTableNames = sinkTableInfo.getRealTableNames();
        this.targetDBs = sinkTableInfo.getTargetDBs();
        this.targetTables = sinkTableInfo.getTargetTables();
        this.fieldNames = sinkTableInfo.getFieldNames();
		this.maxRetries = sinkTableInfo.getMaxRetries();

        // this.ddlUDFMap = rdbTableInfo.getDdlUDFMap();
        if (null != sinkTableInfo.getAutoCommit()) {
            this.autoCommit = Boolean.parseBoolean(sinkTableInfo.getAutoCommit());
        }

        buildSql(tableName, fields, primaryKeys);
        buildSqlTypes(fieldTypeArray);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SinkFunction<RowData> rowDataSinkFunction = createSinkFunction(sinkTableInfo, context);
        int sinkParallelism = sinkTableInfo.getParallelism();
        return SinkFunctionProviderWithParallel.of(rowDataSinkFunction, sinkParallelism);
    }

    @Override
    public String asSummaryString() {
        return "Bsql-Dynamic-" + sinkTableInfo.getName();
    }

    public abstract void buildSql(String tableName, List<String> fields, List<String> primaryKeys);

    public abstract String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField);

    public abstract RetractJDBCOutputFormat getOutputFormat(Context context);

    public abstract String getDriverName();

    public SinkFunction<RowData> createSinkFunction(RdbSinkTableInfo sinkTableInfo, Context context) {
        initParams(sinkTableInfo);
        if (driverName == null || dbURL == null || userName == null
                || password == null || sqlTypes == null || tableName == null) {
            throw new RuntimeException("any of params in(driverName, dbURL, userName, password, type, tableName) " +
                    " must not be null. please check it!!!");
        }
        RetractJDBCOutputFormat outputFormat = getOutputFormat(context);
        outputFormat.setDbURL(dbURL);
        outputFormat.setDrivername(driverName);
        outputFormat.setUsername(userName);
        outputFormat.setPassword(password);
        outputFormat.setTps(tps);
        outputFormat.setInsertQuery(sql);
        outputFormat.setTypesArray(sqlTypes);
        outputFormat.setTableName(tableName);
        outputFormat.setDbType(dbType);
        outputFormat.setInsertType(insertType);
        outputFormat.setRealTableNames(realTableNames);
        outputFormat.setTargetDBs(targetDBs);
        outputFormat.setTargetTables(targetTables);
        outputFormat.setDdlUDFMap(ddlUDFMap);
        outputFormat.setAutoCommit(autoCommit);
        outputFormat.setBatchMaxCount(batchMaxCount);
        outputFormat.setBatchMaxTimeout(batchMaxTimeout);
		outputFormat.setMaxRetries(maxRetries);

        List<Integer> noPrimaryKeysIndex = new ArrayList<>(primaryKeys.size());
        List<Integer> primaryKeysIndex = new ArrayList<>(primaryKeys.size());
        for (int i = 0; i < fieldNames.length; i++) {
            if (!primaryKeys.contains(fieldNames[i])) {
                noPrimaryKeysIndex.add(i);
            } else {
                primaryKeysIndex.add(i);
            }
        }
        outputFormat.setNoPrimaryKeysIndex(noPrimaryKeysIndex);
        outputFormat.setPrimaryKeysIndex(primaryKeysIndex);
        outputFormat.setDbSink(this);
        outputFormat.verifyField();
        return outputFormat;
    }


    /**
     * By now specified class type conversion.
     * FIXME Follow-up has added a new type of time needs to be modified
     * // TODO Need to verify type mapping
     *
     * @param fieldTypeArray
     */
    protected void buildSqlTypes(List<LogicalType> fieldTypeArray) {
        int[] tmpFieldsType = new int[fieldTypeArray.size()];
        for (int i = 0; i < fieldTypeArray.size(); i++) {
            LogicalType logicalType = fieldTypeArray.get(i);
            String fieldType = logicalType.getClass().getName();
            if (fieldType.equals(IntType.class.getName())) {
                tmpFieldsType[i] = Types.INTEGER;
            } else if (fieldType.equals(BooleanType.class.getName())) {
                tmpFieldsType[i] = Types.BOOLEAN;
            } else if (fieldType.equals(BigIntType.class.getName())) {
                tmpFieldsType[i] = Types.BIGINT;
            } else if (fieldType.equals(TinyIntType.class.getName())) {
                tmpFieldsType[i] = Types.TINYINT;
            } else if (fieldType.equals(SmallIntType.class.getName())) {
                tmpFieldsType[i] = Types.SMALLINT;
            } else if (fieldType.equals(VarCharType.class.getName())) {
                tmpFieldsType[i] = Types.CHAR;
            } else if (fieldType.equals(TinyIntType.class.getName())) {
                tmpFieldsType[i] = Types.BINARY;
            } else if (fieldType.equals(FloatType.class.getName())) {
                tmpFieldsType[i] = Types.FLOAT;
            } else if (fieldType.equals(DoubleType.class.getName())) {
                tmpFieldsType[i] = Types.DOUBLE;
            } else if (fieldType.equals(TimestampType.class.getName())) {
                tmpFieldsType[i] = Types.TIMESTAMP;
            } else if (fieldType.equals(DecimalType.class.getName())) {
                tmpFieldsType[i] = Types.DECIMAL;
            } else if (fieldType.equals(DateType.class.getName())) {
                tmpFieldsType[i] = Types.DATE;
            } else if (fieldType.equals(ArrayType.class.getName())) {
                List<LogicalType> child = logicalType.getChildren();
                for (LogicalType childType : child) {
                    if (childType.getClass().getName().equals(IntType.class.getName())) {
                        tmpFieldsType[i] = CLICK_INT_ARRAY;
                        break;
                    } else if (childType.getClass().getName().equals(FloatType.class.getName())) {
                        tmpFieldsType[i] = CLICK_FLOAT_ARRAY;
                        break;
                    } else if (childType.getClass().getName().equals(VarCharType.class.getName())) {
                        tmpFieldsType[i] = CLICK_STRING_ARRAY;
                        break;
                    }
                }
            } else if (fieldType.equals(LocalZonedTimestampType.class.getName())) {
                tmpFieldsType[i] = Types.TIMESTAMP;
            } else {
                throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
            }
        }
        this.sqlTypes = tmpFieldsType;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }
}
