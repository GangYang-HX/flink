package org.apache.flink.bilibili.starter;

import org.apache.flink.bilibili.catalog.BilibiliCatalog;
import org.apache.flink.bilibili.sql.SqlRegister;
import org.apache.flink.bilibili.sql.SqlTree;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkNotNull;
/**
 * 工具类starter
 *
 * @author zhangyang
 * @Date:2021/5/31
 * @Time:3:36 下午
 */
public class ToolBoxStarter {

	public static void main(String[] args) throws Exception {
		/**
		 * input: /rootPath/saberJobId/parseFile
		 *
		 * output:
		 * /rootPath/saberJobId/explain
		 * /rootPath/saberJobId/physicalPlan
		 * /rootPath/saberJobId/lineAnalysis
		 */
		if (args.length != 1) {
			System.err.println("Usage: Please input one params: 1)one file path");
			System.exit(-1);
		}
		String inputFile = args[0];
		// get file context
		String context = readFile(inputFile);
		checkState(!context.isEmpty(), "File must not be empty.");

		// parse file context
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(context);
		String url = root.get("jarUrl").asText();
		JsonNode methodNameNode = root.get("methodName");
		checkNotNull(url, "url must not be empty.");
		checkNotNull(methodNameNode, "methodName must not be empty.");
		checkState(methodNameNode.isArray(), "methodName must be array.");

		// load jar
		String[] split = url.split(",");
		URL[] libs = new URL[split.length];
		for (int i = 0; i < libs.length; i++) {
			libs[i] = new URL("file:///" + split[i]);
		}
		ClassLoader loader = new URLClassLoader(libs, Thread.currentThread().getContextClassLoader());
		Thread.currentThread().setContextClassLoader(loader);

		// executor method
		ToolBoxStarter toolBoxStarter = new ToolBoxStarter();
		ArrayNode methNameArrayNode = (ArrayNode) methodNameNode;
		File file = new File(inputFile);
		File parentFile = file.getParentFile();
		for (int i = 0; i < methNameArrayNode.size(); i++) {
			String methodName = methNameArrayNode.get(i).asText();
			String result = "";
			try {
				switch (methodName) {
					case "explain":
						result = toolBoxStarter.explain(context);
						break;
					case "physicalPlan":
						result = toolBoxStarter.physicalPlan(context);
						break;
					case "lineAnalysis":
						result = toolBoxStarter.lineAnalysis(context);
						break;
					default:
						throw new RuntimeException("not supported method name:" + methodName);
				}
			} catch (Exception e) {
				result = parseStackTrac(e, "");
			}
			bufferedWriterMethod(parentFile.getPath() + "/" + methodName, result);
		}
	}

	private static String parseStackTrac(Throwable e, String rootMessage) {
		StackTraceElement[] stackTrace = e.getStackTrace();
		StringBuilder builder = new StringBuilder();
		builder.append(rootMessage);
		builder.append(e.toString()).append("\n");
		for (StackTraceElement stackTraceElement : stackTrace) {
			builder.append("    ").append(stackTraceElement.toString()).append("\n");
		}
		if (e.getCause() != null) {
			return parseStackTrac(e.getCause(), builder.toString());
		} else {
			return builder.toString();
		}
	}

	private static String readFile(String filepath) throws IOException {
		File file = new File(filepath);
		if (!file.exists()) {
			return "";
		}
		StringBuilder fileContext = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
			char[] buf = new char[1024];
			int numRead = 0;
			while ((numRead = reader.read(buf)) != -1) {
				String readData = String.valueOf(buf, 0, numRead);
				fileContext.append(readData);
				buf = new char[1024];
			}
		}
		return fileContext.toString();
	}

	private static void bufferedWriterMethod(String filepath, String content) throws IOException {
		File file = new File(filepath);
		File parentFile = file.getParentFile();
		if (file.exists()) {
			file.delete();
		}
		if (!parentFile.exists() && !file.isDirectory()) {
			parentFile.mkdirs();
		}
		file.createNewFile();

		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file))) {
			bufferedWriter.write(content);
		}
	}

	public String explain(String context) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(context);
		String sql = root.get("sql").asText();
		SqlTree sqlTree = new SqlTree(sql);
        TableConfig config = new TableConfig();
        config.addConfiguration(getCustomConfig(root.get("customClusterConfig")));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, config);
		tabEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));
		StatementSet statementSet = tabEnv.createStatementSet();
		SqlRegister register = new SqlRegister(env, tabEnv, statementSet);
		register.register(sqlTree);
		return statementSet.explain();
	}

	public String physicalPlan(String context) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(context);
		String sql = root.get("sql").asText();
		SqlTree sqlTree = new SqlTree(sql);
        TableConfig config = new TableConfig();
        config.addConfiguration(getCustomConfig(root.get("customClusterConfig")));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironmentImpl tabEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env, config);
		tabEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));
		StatementSet statementSet = tabEnv.createStatementSet();
        SqlRegister register = new SqlRegister(env, tabEnv, statementSet);
        register.register(sqlTree);
		Parser parser = tabEnv.getPlanner().getParser();
		List<PhysicalExecutionPlan> plan = tabEnv.getPlanner().getPhysicalExecutionPlan(buildOperation(sqlTree, parser));
		StringWriter writer = new StringWriter();
		mapper.writeValue(writer, plan);
		return writer.toString();
	}

	public String lineAnalysis(String context) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(context);
		String sql = root.get("sql").asText();
		SqlTree sqlTree = new SqlTree(sql);
        TableConfig config = new TableConfig();
        config.addConfiguration(getCustomConfig(root.get("customClusterConfig")));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironmentImpl tabEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env, config);
		tabEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));
		StatementSet statementSet = tabEnv.createStatementSet();
        SqlRegister register = new SqlRegister(env, tabEnv, statementSet);
        register.register(sqlTree);
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
