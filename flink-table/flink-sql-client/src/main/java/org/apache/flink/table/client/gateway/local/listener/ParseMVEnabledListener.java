package org.apache.flink.table.client.gateway.local.listener;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * listener to enable mv hint. parse from sql contains specific hint and try to enable or disable
 * hint.
 */
public class ParseMVEnabledListener implements ParserListener {

    public ParseMVEnabledListener(Configuration flinkConfig) {}

    protected static final List<DateTimeFormatter> DATE_TIME_FORMATTER_LIST =
            Arrays.asList(
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    private static final Logger LOG = LoggerFactory.getLogger(ParseMVEnabledListener.class);

    @Override
    public void onParsed(Object... parsed) {
        if (parsed.length < 3
                || !((parsed[0] instanceof SqlNode)
                        && (parsed[1] instanceof Supplier)
                        && (parsed[2] instanceof CatalogManager))) {
            return;
        }
        SqlNode sqlNode = (SqlNode) parsed[0];
        Supplier<FlinkPlannerImpl> validatorSupplier = (Supplier<FlinkPlannerImpl>) parsed[1];
        CatalogManager catalogManager = (CatalogManager) parsed[2];
        parseAndConfigMaterializationEnabled(sqlNode, validatorSupplier.get(), catalogManager);
    }

    /** only support the outermost hint，and decide to reuse MaterializationView or not. */
    private void parseAndConfigMaterializationEnabled(
            final SqlNode sqlNode, FlinkPlannerImpl planner, CatalogManager catalogManager) {
        Optional<Tuple2<String, Boolean>> mvEnabledOpt =
                parseMaterializationHint(
                        sqlNode,
                        OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_ENABLED.key());
        FlinkContextImpl context = (FlinkContextImpl) planner.config().getContext();
        boolean mvEnabledFromUser =
                context.getTableConfig()
                        .getConfiguration()
                        .getBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_ENABLED);
        boolean mvEnabledResult;
        if ((!mvEnabledOpt.isPresent() && mvEnabledFromUser)
                || (mvEnabledOpt.isPresent() && mvEnabledOpt.get().f1)) {
            Optional<Boolean> enabledResultOpt =
                    canReuseMvBasedOnHudiWaterMark(context, sqlNode, catalogManager);
            mvEnabledResult = enabledResultOpt.orElse(false);
        } else {
            mvEnabledResult = false;
        }
        context.getTableConfig()
                .getConfiguration()
                .setBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_ENABLED.key(),
                        mvEnabledResult);
        LOG.info("parse mv listener reset materialization enabled to: {}", mvEnabledResult);
    }

    public static Optional<Tuple2<String, Boolean>> parseMaterializationHint(
            final SqlNode sqlNode, String configKey) {
        SqlNodeList hints = null;
        if (sqlNode instanceof SqlRichExplain) {
            SqlNode statement = ((SqlRichExplain) sqlNode).getStatement();
            if (statement instanceof SqlSelect) {
                hints = ((SqlSelect) statement).getHints();
            }
        } else if (sqlNode instanceof SqlSelect) {
            hints = ((SqlSelect) sqlNode).getHints();
        }

        AtomicReference<Optional<Tuple2<String, Boolean>>> mvEnabled =
                new AtomicReference<>(Optional.empty());
        if (hints != null) {
            hints.getList()
                    .forEach(
                            hint -> {
                                Map<String, String> optionKVPairs =
                                        ((SqlHint) hint).getOptionKVPairs();
                                if (optionKVPairs.containsKey(configKey)) {
                                    mvEnabled.set(
                                            Optional.of(
                                                    Tuple2.of(
                                                            sqlNode.toString(),
                                                            Boolean.parseBoolean(
                                                                    optionKVPairs.get(
                                                                            configKey)))));
                                }
                            });
        }
        return mvEnabled.get();
    }

    /** compare window_start with ctime, window_end with watermark. */
    private Optional<Boolean> canReuseMvBasedOnHudiWaterMark(
            FlinkContextImpl context, SqlNode sqlNode, CatalogManager catalogManager) {
        Boolean defaultPrefer =
                context.getTableConfig()
                        .getConfiguration()
                        .get(OptimizerConfigOptions.TABLE_OPTIMIZER_PREFER_MATERIALIZATION);
        try {
            RuntimeExecutionMode executionMode =
                    context.getTableConfig().getConfiguration().get(ExecutionOptions.RUNTIME_MODE);
            if (executionMode != RuntimeExecutionMode.BATCH) {
                return Optional.of(false);
            }

            Optional<String> windowParams = parseWindowParam(sqlNode, "window_end");
            if (!windowParams.isPresent()) {
                return Optional.of(defaultPrefer);
            }

            // sub-query
            if (sqlNode instanceof SqlRichExplain) {
                SqlNode statement = ((SqlRichExplain) sqlNode).getStatement();
                if (statement instanceof SqlSelect) {
                    sqlNode = statement;
                }
            }

            Set<String> subSqlSet = new HashSet<>();
            Set<Tuple2<String, TimeUnit>> windowIntervals = new HashSet<>();
            if (sqlNode instanceof SqlSelect) {
                SqlNode from = ((SqlSelect) sqlNode).getFrom();
                if (from instanceof SqlBasicCall) {
                    ((SqlBasicCall) from)
                            .getOperandList()
                            .forEach(
                                    operandNode -> {
                                        if (operandNode instanceof SqlSelect) {
                                            SqlNodeList hints =
                                                    ((SqlSelect) operandNode).getHints();
                                            ((SqlSelect) operandNode).setHints(null);
                                            subSqlSet.add(operandNode.toString());
                                            ((SqlSelect) operandNode).setHints(hints);

                                            SqlNode innerFrom = ((SqlSelect) operandNode).getFrom();
                                            List<SqlNode> windowOpList =
                                                    ((SqlBasicCall)
                                                                    ((SqlBasicCall) innerFrom)
                                                                            .getOperandList()
                                                                            .get(0))
                                                            .getOperandList();
                                            for (int i = windowOpList.size() - 1; i >= 0; i--) {
                                                if (windowOpList.get(i)
                                                        instanceof SqlIntervalLiteral) {
                                                    SqlIntervalLiteral.IntervalValue value =
                                                            (SqlIntervalLiteral.IntervalValue)
                                                                    ((SqlIntervalLiteral)
                                                                                    windowOpList
                                                                                            .get(i))
                                                                            .getValue();
                                                    String intervalLiteral =
                                                            value.getIntervalLiteral();
                                                    TimeUnit timeUnit =
                                                            value.getIntervalQualifier()
                                                                    .timeUnitRange
                                                                    .startUnit;
                                                    windowIntervals.add(
                                                            new Tuple2<>(
                                                                    intervalLiteral, timeUnit));
                                                    break;
                                                }
                                            }
                                        }
                                    });
                }
            }

            // compare window start with ctime from hoodie manager
            LocalDateTime now = LocalDateTime.now();
            Optional<LocalDateTime> queryTimeOpt = parseDataTime(windowParams.get());
            if (queryTimeOpt.isPresent()) {
                now = queryTimeOpt.get();
                // window end time.
                context.getTableConfig()
                        .getConfiguration()
                        .setLong(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_MATERIALIZATION_QUERY_FLAG_END_TIME
                                        .key(),
                                now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
            }
            if (windowIntervals.size() > 0) {
                Tuple2<String, TimeUnit> windowIntervalTuple =
                        windowIntervals.toArray(new Tuple2[0])[0];
                long interval = Long.parseLong(windowIntervalTuple.f0);
                switch (windowIntervalTuple.f1.name()) {
                    case "SECOND":
                        now = now.minusSeconds(interval);
                        break;
                    case "MINUTE":
                        now = now.minusMinutes(interval);
                        break;
                    case "HOUR":
                        now = now.minusHours(interval);
                        break;
                    case "DAY":
                        now = now.minusDays(interval);
                        break;
                    case "MONTH":
                        now = now.minusMonths(interval);
                        break;
                    case "YEAR":
                        now = now.minusYears(interval);
                        break;
                    default:
                }
            }
            // window start time.
            long queryParamTime = now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            List<Tuple3<ObjectIdentifier, String, Long>> allMv =
                    catalogManager.getAllMaterializedViewObjectsForRewriting(queryParamTime);
            context.getTableConfig()
                    .getConfiguration()
                    .setLong(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_MATERIALIZATION_QUERY_FLAG_START_TIME
                                    .key(),
                            queryParamTime);
            AtomicReference<Optional<Boolean>> watermarkSatisfied =
                    new AtomicReference<>(Optional.empty());
            for (Tuple3<ObjectIdentifier, String, Long> entry : allMv) {
                if (subSqlSet.contains(entry.f1)) {
                    String fullTableName = entry.f0.toObjectPath().getFullName();
                    Optional<String> mvWatermarkOpt =
                            catalogManager.getMaterializeViewWatermark(fullTableName);
                    if (mvWatermarkOpt.isPresent() && queryTimeOpt.isPresent()) {
                        LocalDateTime watermarkDateTime =
                                Instant.ofEpochMilli(Long.parseLong(mvWatermarkOpt.get()))
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDateTime();
                        // TODO remove
                        LocalDateTime tmpWatermarkDateTime = watermarkDateTime.minusHours(8L);
                        // compare window end with mv table watermark
                        LOG.info(
                                "compare query time with watermark, tmpWatermarkDateTime: {}, queryTimeOpt: {}.",
                                tmpWatermarkDateTime,
                                queryTimeOpt);
                        watermarkSatisfied.set(
                                Optional.of(
                                        queryTimeOpt.get().compareTo(tmpWatermarkDateTime) <= 0));
                    }
                    break;
                }
            }
            if (!watermarkSatisfied.get().isPresent()) {
                LOG.warn("no mvSql matched, fallback to origin table.");
            }
            return watermarkSatisfied.get();
        } catch (Exception e) {
            LOG.error(
                    "failed to reuse mv based on hudi watermark. fallback to default config option.",
                    e);
        }

        return Optional.of(defaultPrefer);
    }

    /** parse window_end value from sqlNode, such as 2022-09-09 15:45:00.000. */
    private Optional<String> parseWindowParam(final SqlNode sqlNode, String paramName) {
        SqlNode where = null;
        if (sqlNode instanceof SqlRichExplain) {
            SqlNode statement = ((SqlRichExplain) sqlNode).getStatement();
            if (statement instanceof SqlSelect) {
                where = ((SqlSelect) statement).getWhere();
            }
        } else if (sqlNode instanceof SqlSelect) {
            where = ((SqlSelect) sqlNode).getWhere();
        }

        AtomicReference<Optional<String>> windowParamMatched =
                new AtomicReference<>(Optional.empty());
        extractWindowParamRecursively(where, paramName, windowParamMatched);
        return windowParamMatched.get();
    }

    private void extractWindowParamRecursively(
            SqlNode operandNode,
            String paramName,
            AtomicReference<Optional<String>> windowParamMatched) {
        if (operandNode instanceof SqlBasicCall) {
            SqlNode[] operands = ((SqlBasicCall) operandNode).getOperands();
            if (operands.length == 2) {
                if (operands[0] instanceof SqlIdentifier
                        && operands[0].toString().equals(paramName)) {
                    extractWindowParam(operands[1], windowParamMatched);
                } else if (operands[1] instanceof SqlIdentifier
                        && operands[1].toString().equals(paramName)) {
                    extractWindowParam(operands[0], windowParamMatched);
                } else {
                    extractWindowParamRecursively(operands[0], paramName, windowParamMatched);
                    extractWindowParamRecursively(operands[1], paramName, windowParamMatched);
                }
            } else {
                for (SqlNode operand : operands) {
                    extractWindowParamRecursively(operand, paramName, windowParamMatched);
                }
            }
        }
    }

    private void extractWindowParam(
            SqlNode second, AtomicReference<Optional<String>> windowParamMatched) {
        if (second instanceof SqlCall && !((SqlCall) second).getOperandList().isEmpty()) {
            SqlNode innerNode = ((SqlCall) second).getOperandList().get(0);

            // TODO for dqc
            if (((SqlCall) second).getOperator().getName().equalsIgnoreCase("from_unixtime")) {
                if (innerNode instanceof SqlBasicCall
                        && !((SqlBasicCall) innerNode).getOperandList().isEmpty()) {
                    SqlNode timeNodeInUdf = ((SqlBasicCall) innerNode).getOperandList().get(0);
                    if (timeNodeInUdf instanceof SqlNumericLiteral) {
                        try {
                            String unixTimeInUdf =
                                    ((SqlNumericLiteral) timeNodeInUdf).getValue().toString();
                            LocalDateTime unixTimeLocalDateTime =
                                    Instant.ofEpochMilli(Long.parseLong(unixTimeInUdf))
                                            .atZone(ZoneId.systemDefault())
                                            .toLocalDateTime();
                            windowParamMatched.set(
                                    Optional.of(
                                            DATE_TIME_FORMATTER_LIST
                                                    .get(0)
                                                    .format(unixTimeLocalDateTime)));
                        } catch (Exception e) {
                            LOG.error("parse time from from_unixtime udf failed.", e);
                        }
                        return;
                    }
                }
            }

            if (innerNode instanceof SqlLiteral) {
                second = innerNode;
            }
        }

        if (second instanceof SqlLiteral && ((SqlLiteral) second).getValue() instanceof NlsString) {
            windowParamMatched.set(
                    Optional.of(((NlsString) ((SqlLiteral) second).getValue()).getValue()));
        }
    }

    private Optional<LocalDateTime> parseDataTime(String timeStr) {
        for (DateTimeFormatter formatter : DATE_TIME_FORMATTER_LIST) {
            try {
                return Optional.of(LocalDateTime.parse(timeStr, formatter));
            } catch (DateTimeParseException ignored) {
            }
        }

        return Optional.empty();
    }
}
