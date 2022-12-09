package org.apache.flink.bilibili.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.bilibili.udf.BuildInFunc;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhangyang
 * @Date:2021/5/31
 * @Time:5:44 下午
 */
public class SqlRegister {

	private Logger LOG = LoggerFactory.getLogger(SqlRegister.class);

	private static final String WK_PATTERN_STR = "(?i)\\s*WATERMARK\\s+for\\s+(\\S+)\\s+as\\s+(\\S+)";
	private static final Pattern WK_PATTERN = Pattern.compile(WK_PATTERN_STR);

	private static final String LINE = "line";
	private static final String LINE_IN_ERROR_MSG = LINE + "\\s\\d+,";
	private static final Pattern LINE_IN_ERROR_MSG_PATTERN = Pattern.compile(LINE_IN_ERROR_MSG);
	private final StreamExecutionEnvironment env;
	private final StreamTableEnvironment tabEnv;
	private final StatementSet statementSet;

	public SqlRegister(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv, StatementSet statementSet) {
		this.env = env;
		this.tabEnv = tabEnv;
		this.statementSet = statementSet;
	}

	public void register(SqlTree tree) throws Exception {
		registerCommonUdf();
		registerUserUdf(tree);
		registerCreateTable(tree);
		registerCreateView(tree);
		registerInsert(tree);
	}

	private void registerCommonUdf() {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		for (String funcName : BuildInFunc.FUNC_MAP.keySet()) {
			Class<? extends UserDefinedFunction> funcClass = BuildInFunc.FUNC_MAP.get(funcName);
            try {
                UserDefinedFunction funcObject = (UserDefinedFunction) Class.forName(funcClass.getName(), false, loader).newInstance();
                if(funcObject instanceof TableFunction){
                    tabEnv.registerFunction(funcName,(TableFunction) funcObject);
                }

                if(funcObject instanceof ScalarFunction){
                    tabEnv.registerFunction(funcName,(ScalarFunction) funcObject);
                }

                if(funcObject instanceof AggregateFunction){
                    tabEnv.registerFunction(funcName,(AggregateFunction) funcObject);
                }
            } catch (Exception e) {
                LOG.error("register func error,funcName:{},msg:{}", funcName, e);
            }
		}
	}

	private void registerUserUdf(SqlTree tree) throws Exception {
		List<String> udfSqlList = tree.getCreateUdfSqlList();
		for (String udf : udfSqlList) {
			try {
				tabEnv.executeSql(udf);
			} catch (Exception e) {
				parsePositionException(e, tree.getOriginMultiSql(), udf);
			}
		}
	}

	private void registerCreateTable(SqlTree tree) throws Exception {
		List<String> createSqlList = tree.getCreateTableSqlList();
		for (String createSql : createSqlList) {
			try {
				tabEnv.executeSql(createSql);
			} catch (Exception e) {
				parsePositionException(e, tree.getOriginMultiSql(), createSql);
			}
			if (WK_PATTERN.matcher(createSql).find()) {
				env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			}
		}
	}

	private void registerCreateView(SqlTree tree) throws Exception {
		List<String> createViewList = tree.getCreateViewSqlList();
		for (String createView : createViewList) {
			try {
				tabEnv.executeSql(createView);
			} catch (RuntimeException e) {
				parsePositionException(e, tree.getOriginMultiSql(), createView);
			}
		}
	}

	private void registerInsert(SqlTree tree) throws Exception {
		List<String> insertSqlList = tree.getInsertSqlList();
		for (String insertSql : insertSqlList) {
			try {
				statementSet.addInsertSql(insertSql);
			} catch (Exception e) {
				parsePositionException(e, tree.getOriginMultiSql(), insertSql);
			}
		}
	}

	/**
	 * 解析行号报错，将单SQL行号替换成全SQL中的行号
	 * 如果异常被替换，则通过LOG记录原始exception
	 */
	private void parsePositionException(Exception e, String originMultiSql, String sql) throws Exception {
		String errMsg = "";
		if (e instanceof SqlParserException) {
			SqlParserException sqlParserException = (SqlParserException) e;
			errMsg = sqlParserException.getMessage();
		} else if (e instanceof ValidationException) {
			ValidationException validationException = (ValidationException) e;
			errMsg = validationException.getMessage();
		} else {
			throw e;
		}

		try {
			// line number + error line number in current sql -> final error line number
			String sqlBefore = originMultiSql.substring(0, originMultiSql.indexOf(sql));
			int lineBefore = countIgnoreQuote(sqlBefore, "\n");
			errMsg = parseErrorMsg(errMsg, lineBefore, e);
		} catch (Exception exception) {
			LOG.error("unexpected exception when replace error msg, throw original exception.", exception);
			throw e;
		}

		LOG.error("sql解析出错: \n{}", StringUtils.join(ExceptionUtils.getRootCauseStackTrace(e), "\n"));
		throw new RuntimeException(errMsg);
	}

	private String parseErrorMsg(String errorMsg, Integer lineBefore, Exception e) throws Exception {
		Matcher matcher = LINE_IN_ERROR_MSG_PATTERN.matcher(errorMsg);
		if (!matcher.find()) {
			LOG.error("line number pattern not found in error message, throw original exception.");
			throw e;
		}

		do {
			String lineNumberWithComma = matcher.group();
			String[] split = lineNumberWithComma.split("\\s");
			String lineNumber = split[split.length - 1].substring(0, split[split.length - 1].length() - 1);
			String modifiedLineAndNumber = LINE + " " + (Integer.parseInt(lineNumber) + lineBefore) + ",";
			errorMsg = errorMsg.replace(lineNumberWithComma, modifiedLineAndNumber);
		} while (matcher.find());

		return errorMsg;
	}

	private static int countIgnoreQuote(String sql, String flag) {
		if (sql == null || sql.isEmpty()) {
			return 0;
		}

		boolean inQuotes = false;
		boolean inSingleQuotes = false;
		int count = 0;

		for (int i = 0; i < sql.length(); i++) {
			char c = sql.charAt(i);

			if (c == '\"' && !inSingleQuotes) {
				inQuotes = !inQuotes;
				continue;
			}
			if (c == '\'' && !inQuotes) {
				inSingleQuotes = !inSingleQuotes;
				continue;
			}

			// 在引号里面就直接过过过
			if (inQuotes || inSingleQuotes) {
				continue;
			}
			String sub = sql.substring(i, Math.min(i + flag.length(), sql.length()));
			if (sub.equals(flag)) {
				count++;
				i += flag.length() - 1;
			}
		}
		return count;
	}

}
