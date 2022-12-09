package com.bilibili.bsql.rdb.format;

import com.bilibili.bsql.common.format.BatchOutputFormat;
import com.bilibili.bsql.common.utils.JDBCLock;
import com.bilibili.bsql.rdb.table.BsqlRdbDynamicSinkBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * OutputFormat to write tuples into a database. The OutputFormat has to be configured using the supplied
 * OutputFormatBuilder.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RetractJDBCOutputFormat.java
 * @description This is the description of RetractJDBCOutputFormat.java
 * @createTime 2020-10-20 15:36:00
 */
public abstract class RetractJDBCOutputFormat extends BatchOutputFormat {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RetractJDBCOutputFormat.class);

    protected String username;
    protected String password;
    protected String drivername;
    protected String dbURL;
    protected String tableName;
    protected String dbType;
    protected BsqlRdbDynamicSinkBase dbSink;
    protected List<Integer> noPrimaryKeysIndex;
    protected List<Integer> primaryKeysIndex;
    protected String insertQuery;
    protected int insertType = 0;
    protected int[] typesArray;
    protected Connection dbConn;
    protected PreparedStatement upload;
    protected Map<String, List<String>> realIndexes = Maps.newHashMap();
    protected List<String> fullField = Lists.newArrayList();
    protected String realTableNames;
    protected String tps;
    protected final Properties defaultConnectionConfig = new Properties();
    protected String targetTables;
    protected String targetDBs;
    @Deprecated
    private Map ddlUDFMap;
    protected boolean autoCommit = true;
	public static final int CONNECTION_CHECK_TIMEOUT_SECONDS = 60;
	protected Integer maxRetries;

    public RetractJDBCOutputFormat(DataType dataType, DynamicTableSink.Context context) {
        super(dataType,context);
        defaultConnectionConfig.put("autoReconnect", "true");
        defaultConnectionConfig.put("autoReconnectForPools", "true");
        //真正开启statement批量发送
        defaultConnectionConfig.put("rewriteBatchedStatements", "true");
        //数据传输压缩
        defaultConnectionConfig.put("useCompression", "true");
    }

    public String sinkType() {
        // overwrite this method if needed.
        return "";
    }

    @Override
    public void doOpen(int taskNumber, int numTasks) throws IOException {
        super.doOpen(taskNumber, numTasks);
        try {
            establishConnection();
            if (dbConn.getMetaData().getTables(null, null, tableName, null).next()) {
                if (isReplaceInsertQuery()) {
                    insertQuery = dbSink.buildUpdateSql(tableName, Arrays.asList(dbSink.getFieldNames()), realIndexes, fullField);
                }
                if (!autoCommit) {
                    dbConn.setAutoCommit(false);
                    LOG.info("set autoCommit=false");
                }
                upload = dbConn.prepareStatement(insertQuery);
            } else {
                throw new SQLException("Table " + tableName + " doesn't exist");
            }
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    protected Connection establishConnection() throws SQLException, ClassNotFoundException {
        LOG.info("Jdbc driver = {}", drivername);
        LOG.info("Jdbc url = {}", dbURL);
        LOG.info("Jdbc username = {}", username);
        LOG.info("Jdbc password = {}", password);
        for (Map.Entry<Object, Object> entry : defaultConnectionConfig.entrySet()) {
            LOG.info("Jdbc Config key = {}, value = {}", entry.getKey(), entry.getValue());
        }
		synchronized (JDBCLock.class) {
			Class.forName(drivername);
		}
        if (username == null) {
            dbConn = DriverManager.getConnection(dbURL, defaultConnectionConfig);
        } else {
            defaultConnectionConfig.put("user", username);
            defaultConnectionConfig.put("password", password);
            dbConn = DriverManager.getConnection(dbURL, defaultConnectionConfig);
        }
        LOG.info("Jdbc connector open successful. User = {}, Pass = {}", username, password);
		return dbConn;
    }

	protected Connection reestablishConnection() throws SQLException, ClassNotFoundException {
		try {
			dbConn.close();
		} catch (SQLException e) {
			LOG.info("JDBC connection close failed.", e);
		} finally {
			dbConn = null;
		}
			dbConn = this.establishConnection();
		return dbConn;
	}

    @Override
    protected void doWrite(RowData record) {
        if (typesArray != null && typesArray.length > 0 && typesArray.length != record.getArity()) {
            LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }
        try {
            // RowKind.UPDATE_AFTER means retract
            if (!record.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
                if (insertType == 0) {
                    updateOrInsertPreparedStmt(record, upload);
                } else if (insertType == 1) {
                    insertOrIgnore(record, upload);
                }
                upload.addBatch();
            } else {
                // do nothing
            }
        } catch (SQLException | IllegalArgumentException e) {
            throw new IllegalArgumentException("writeRecord() failed", e);
        }
    }

    @Override
    protected void flush() throws Exception {
        upload.executeBatch();
        if (!autoCommit) dbConn.commit();
    }

    public abstract void insertOrIgnore(RowData rowData, PreparedStatement preparedStatement) throws SQLException;

    public abstract void updateOrInsertPreparedStmt(RowData row, PreparedStatement preparedStatement) throws SQLException;

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        super.close();
        try {
            writeLock.lock();
            if (upload != null) {
                upload.executeBatch();
                if (!autoCommit) dbConn.commit();
                upload.close();
            }
        } catch (SQLException se) {
            LOG.info("input format couldn't be closed - " + se.getMessage());
        } finally {
            upload = null;
            writeLock.unlock();
        }

        try {
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException se) {
            LOG.info("input format couldn't be closed - " + se.getMessage());
        } finally {
            dbConn = null;
        }
    }

    public boolean isReplaceInsertQuery() throws SQLException {
        return false;
    }

    public void verifyField() {
        if (StringUtils.isBlank(username)) {
            LOG.info("Username was not supplied separately.");
        }
        if (StringUtils.isBlank(password)) {
            LOG.info("Password was not supplied separately.");
        }
        if (StringUtils.isBlank(dbURL)) {
            throw new IllegalArgumentException("No database URL supplied.");
        }
        if (StringUtils.isBlank(insertQuery)) {
            throw new IllegalArgumentException("No insertQuery supplied");
        }
        if (StringUtils.isBlank(drivername)) {
            throw new IllegalArgumentException("No driver supplied");
        }
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTps() {
        return tps;
    }

    public void setTps(String tps) {
        this.tps = tps;
    }

    public void setDrivername(String drivername) {
        this.drivername = drivername;
    }

    public void setDbURL(String dbURL) {
        this.dbURL = dbURL;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setDbSink(BsqlRdbDynamicSinkBase dbSink) {
        this.dbSink = dbSink;
    }


    public void setInsertQuery(String insertQuery) {
        this.insertQuery = insertQuery;
    }

    public void setTypesArray(int[] typesArray) {
        this.typesArray = typesArray;
    }

    public String getDbType() {
        return dbType;
    }

    public BsqlRdbDynamicSinkBase getDbSink() {
        return dbSink;
    }

    public Connection getDbConn() {
        return dbConn;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, List<String>> getRealIndexes() {
        return realIndexes;
    }

    public List<String> getFullField() {
        return fullField;
    }

    public List<Integer> getNoPrimaryKeysIndex() {
        return noPrimaryKeysIndex;
    }

    public void setNoPrimaryKeysIndex(List<Integer> noPrimaryKeysIndex) {
        this.noPrimaryKeysIndex = noPrimaryKeysIndex;
    }

    public int getInsertType() {
        return insertType;
    }

    public void setInsertType(int insertType) {
        this.insertType = insertType;
    }

    public String getRealTableNames() {
        return realTableNames;
    }

    public void setRealTableNames(String realTableNames) {
        this.realTableNames = realTableNames;
    }

    public String getTargetTables() {
        return targetTables;
    }

    public void setTargetTables(String targetTables) {
        this.targetTables = targetTables;
    }

    public String getTargetDBs() {
        return targetDBs;
    }

    public void setTargetDBs(String targetDBs) {
        this.targetDBs = targetDBs;
    }

    public Map getDdlUDFMap() {
        return ddlUDFMap;
    }

    public void setDdlUDFMap(Map ddlUDFMap) {
        this.ddlUDFMap = ddlUDFMap;
    }

    public List<Integer> getPrimaryKeysIndex() {
        return primaryKeysIndex;
    }

    public void setPrimaryKeysIndex(List<Integer> primaryKeysIndex) {
        this.primaryKeysIndex = primaryKeysIndex;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

	public Integer getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(Integer maxRetries) {
		this.maxRetries = maxRetries;
	}
}
