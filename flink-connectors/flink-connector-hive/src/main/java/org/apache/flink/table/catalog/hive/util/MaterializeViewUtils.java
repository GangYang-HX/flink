package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/** read from hudi manager db. */
public class MaterializeViewUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializeViewUtils.class);

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String QUERY_SQL =
            "select sink_table_name, origin_job_sql, DATE_FORMAT(ctime,'%%Y-%%m-%%d %%T') AS ctime from t_materialized_clustering_job where is_deleted = 0 and ctime <= '%s';";

    private static volatile Connection conn;

    private final ReadableConfig configuration;

    public MaterializeViewUtils(ReadableConfig configuration) {
        this.configuration = configuration;
        init();
        registerShutdownHook();
    }

    private void init() {
        if (conn == null) {
            synchronized (MaterializeViewUtils.class) {
                if (conn == null) {
                    LOG.info("init MaterializeViewUtils, connect to with hoodie manager db.");
                    String dbUrl =
                            configuration.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_DB_URL);
                    String user =
                            configuration.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_USER);
                    String pwd =
                            configuration.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_PWD);

                    if (StringUtils.isBlank(dbUrl)
                            || StringUtils.isBlank(user)
                            || StringUtils.isBlank(pwd)) {
                        String errMsg =
                                String.format(
                                        "init materialize view util failed, please check config %s, %s, %s.",
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_DB_URL
                                                .key(),
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_USER
                                                .key(),
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_PWD
                                                .key());
                        LOG.error(errMsg);
                        throw new RuntimeException(errMsg);
                    }
                    try {
                        Class.forName(JDBC_DRIVER);
                        conn = DriverManager.getConnection(dbUrl, user, pwd);
                    } catch (Exception e) {
                        LOG.error("init MaterializeViewUtils conn failed.", e);
                    }
                }
            }
        }
    }

    public List<Tuple3<String, String, Long>> listAllMVsFromMySQL(Long flagTime) {
        List<Tuple3<String, String, Long>> result = new ArrayList<>();
        try {
            if (configuration.get(ExecutionOptions.RUNTIME_MODE) != RuntimeExecutionMode.BATCH) {
                return result;
            }

            if (flagTime == null) {
                flagTime = System.currentTimeMillis();
            }
            flagTime -=
                    configuration.get(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_MATERIALIZATION_QUERY_DELAY_TIME);
            LocalDateTime flagDateTime =
                    Instant.ofEpochMilli(flagTime).atZone(ZoneId.systemDefault()).toLocalDateTime();

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(QUERY_SQL, flagDateTime));

            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            while (rs.next()) {
                String sinkTableName = rs.getString("sink_table_name");
                String mvSql = rs.getString("origin_job_sql");
                String ctime = rs.getString("ctime");
                long ctimeFromDB =
                        LocalDateTime.parse(ctime, df)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                Tuple3<String, String, Long> rowTuple =
                        new Tuple3<>(sinkTableName, mvSql, ctimeFromDB);
                result.add(rowTuple);
            }
            rs.close();
        } catch (Exception e) {
            LOG.error("list all mv sql failed.", e);
        }

        return result;
    }

    public ReadableConfig getConfiguration() {
        return configuration;
    }

    public void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            LOG.error("close MaterializeViewUtils conn & stmt failed.", e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception ignored) {
            }
        }
    }

    private void registerShutdownHook() {
        Thread shutdownHook = new Thread(this::close);
        ShutdownHookUtil.addShutdownHookThread(
                shutdownHook, MaterializeViewUtils.class.getSimpleName(), LOG);
    }
}
