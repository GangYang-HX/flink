package org.apache.flink.table.client.gateway.local.listener;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.client.gateway.local.utils.KafkaUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** listener to create mv. parse from sql contains specific hint and try to send to kafka topic. */
public class CreateMVListener implements ParserListener {
    private static final Logger LOG = LoggerFactory.getLogger(CreateMVListener.class);
    private final KafkaUtils kafkaUtils;

    public CreateMVListener(Configuration flinkConfig) {
        this.kafkaUtils = new KafkaUtils(flinkConfig);
    }

    @Override
    public void onParsed(Object... parsed) {
        try {
            if (parsed.length < 1 || !(parsed[0] instanceof SqlNode)) {
                return;
            }
            SqlNode sqlNode = (SqlNode) parsed[0];
            // TODO ignore send msg to create mv when explain sql
            if (sqlNode instanceof SqlRichExplain) {
                return;
            }
            List<String> subQueryList = parseSubQuerySql(sqlNode);
            ObjectMapper objectMapper = new ObjectMapper();
            for (String subQuery : subQueryList) {
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.put("sql", subQuery);
                String kafkaMsg = objectMapper.writeValueAsString(objectNode);
                kafkaUtils.sendMsg(kafkaMsg);
            }
        } catch (Exception e) {
            LOG.error("fatal error occurred in create mv listener.", e);
        }
    }

    /** parse all sub-query with sub-query hint. */
    public List<String> parseSubQuerySql(SqlNode sqlNode) {
        List<String> subQueryList = new ArrayList<>();
        parseMaterializationHintRecursively(
                sqlNode,
                OptimizerConfigOptions.TABLE_OPTIMIZER_MATERIALIZATION_SUB_QUERY.key(),
                subQueryList);
        return subQueryList;
    }

    private void parseMaterializationHintRecursively(
            SqlNode sqlNode, String configKey, List<String> subQuerySqlList) {
        Optional<Tuple2<String, Boolean>> parsedOpt =
                ParseMVEnabledListener.parseMaterializationHint(sqlNode, configKey);
        parsedOpt.ifPresent(
                parsed -> {
                    if (parsed.f1) {
                        subQuerySqlList.add(parsed.f0);
                    }
                });

        if (sqlNode instanceof SqlRichExplain) {
            SqlNode statement = ((SqlRichExplain) sqlNode).getStatement();
            if (statement instanceof SqlSelect) {
                sqlNode = statement;
            }
        }

        if (sqlNode instanceof SqlSelect) {
            SqlNode from = ((SqlSelect) sqlNode).getFrom();
            if (from instanceof SqlBasicCall) {
                ((SqlBasicCall) from)
                        .getOperandList()
                        .forEach(
                                operandNode -> {
                                    if (operandNode instanceof SqlSelect) {
                                        parseMaterializationHintRecursively(
                                                operandNode, configKey, subQuerySqlList);
                                    }
                                });
            }
        }
    }
}
