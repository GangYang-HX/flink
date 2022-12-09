package org.apache.flink.bilibili.starter;

import org.apache.flink.bilibili.catalog.BilibiliCatalog;
import org.apache.flink.bilibili.catalog.utils.CatalogUtil;
import org.apache.flink.bilibili.catalog.utils.KafkaUtil;
import org.apache.flink.bilibili.sql.SqlRegister;
import org.apache.flink.bilibili.sql.SqlTree;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.LineAgeInfo;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 工具类starter
 *
 * @author zhangyang
 * @Date:2021/5/31
 * @Time:3:36 下午
 */
public class ToolBoxStarter {

	public String explain(String context) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(context);
        String sql = root.get("sql").asText();
        sql = KafkaUtil.convert(sql);
        SqlTree sqlTree = new SqlTree(sql);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().withConfiguration(getCustomConfig(root.get("customClusterConfig"))).build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
        tabEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));
        CatalogUtil.registerHoodieCatalog(env, tabEnv);
        StatementSet statementSet = tabEnv.createStatementSet();
        SqlRegister register = new SqlRegister(env, tabEnv, statementSet);
        register.register(sqlTree,true);
        return statementSet.explain();
	}

    public String physicalPlan(String context) throws IOException {
        List plan = new ArrayList();
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, plan);
        return writer.toString();
    }

    public String lineAnalysis(String context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(context);
        String sql = root.get("sql").asText();
        sql = KafkaUtil.convert(sql);
        SqlTree sqlTree = new SqlTree(sql);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().withConfiguration(getCustomConfig(root.get("customClusterConfig"))).build();
        StreamTableEnvironmentImpl tabEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
        tabEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));
        CatalogUtil.registerHoodieCatalog(env, tabEnv);
        StatementSet statementSet = tabEnv.createStatementSet();
        SqlRegister register = new SqlRegister(env, tabEnv, statementSet);
        register.register(sqlTree,true);
        Parser parser = tabEnv.getPlanner().getParser();
        List<LineAgeInfo> lineAge = tabEnv.getPlanner().generateLineAge(buildOperation(sqlTree, parser));
        //去重
        Set<LineAgeInfo> lineAgeSet = new HashSet<>(lineAge);
        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, lineAgeSet);
        return writer.toString();
    }

    private List<Operation> buildOperation(SqlTree sqlTree, Parser parser) {
        List<Operation> opList = new ArrayList<>();
        for (String item : sqlTree.getCreateUdfSqlList()) {
            opList.addAll(parser.parse(item));
        }
        for (String item : sqlTree.getCreateViewSqlList()) {
            opList.addAll(parser.parse(item));
        }
        for (String item : sqlTree.getCreateTableSqlList()) {
            opList.addAll(parser.parse(item));
        }
        for (String item : sqlTree.getInsertSqlList()) {
            opList.addAll(parser.parse(item));
        }
        return opList;
    }

    private Configuration getCustomConfig(JsonNode custom) {
        if (custom != null) {
            Configuration customConfig = new Configuration();
            Iterator<String> elements = custom.fieldNames();
            while (elements.hasNext()) {
                String key = elements.next();
                customConfig.setString(key, custom.get(key).asText());
            }
            return customConfig;
        } else {
            return new Configuration();
        }
    }

}
