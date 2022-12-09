package org.apache.flink.bilibili.sql;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.bilibili.catalog.client.KeeperRpc;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author galaxy
 */
public class SqlTree {

	private static final String CREATE_UDF_STR = "(?i)\\s*create\\s+function\\s+(\\S+)\\s+as\\s+(\\S+)\\sLANGUAGE\\s+(\\S+)";
	private static final Pattern CREATE_UDF_PATTERN = Pattern.compile(CREATE_UDF_STR);

	private static final String CREATE_TABLE_STR = "(?i)create\\s+table\\s+(\\S+)\\s*(\\((.+)\\))?\\s*(with\\s*\\((.+)\\))?";
	private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(CREATE_TABLE_STR);

	private static final String CREATE_VIEW_STR = "(?i)create\\s+view\\s+([^\\s]+)\\s+as\\s+select\\s+(.*)";
	private static final Pattern CREATE_VIEW_PATTERN = Pattern.compile(CREATE_VIEW_STR);

	//kafka表匹配正则表达式
	private static final String  KAFKA_TABLE_PATTERN_STR = "\\s`?Kafka_[A-Za-z0-9_-]+`?\\.`?([A-Za-z0-9_-]+)`?(\\.`?([A-Za-z0-9_-]+)`?)?(\\.`?([A-Za-z0-9_-]+)`?)?\\s";
	public static final Pattern KAFKA_TABLE_PATTERN = Pattern.compile(KAFKA_TABLE_PATTERN_STR);

	private static final String INSERT_STR = "insert";

	private static String multiSqlBeforeSplit;
	private final List<String> createTableSqlList;
	private final List<String> createViewSqlList;
	private final List<String> insertSqlList;
	private final List<String> createUdfSqlList;

	private KeeperRpc keeperRpc;

	public SqlTree(String multiSql) {
		//获取keeperRpc单例
		keeperRpc = KeeperRpc.getInstance();

		List<String> sqlList = splitSql(multiSql);
		createTableSqlList = new ArrayList<>();
		createViewSqlList = new ArrayList<>();
		insertSqlList = new ArrayList<>();
		createUdfSqlList = new ArrayList<>();

		for (String sql : sqlList) {
			if (StringUtils.isBlank(sql)) {
				continue;
			}

			//将kafka简化表转置为标准的源.库.表
			sql = kafkaTableConvert(sql).trim();
			if (CREATE_UDF_PATTERN.matcher(sql).find()) {
				createUdfSqlList.add(sql);
				continue;
			}

			if (CREATE_TABLE_PATTERN.matcher(sql).find()) {
				createTableSqlList.add(sql);
				continue;
			}
			if (CREATE_VIEW_PATTERN.matcher(sql).find()) {
				createViewSqlList.add(sql);
				continue;
			}
			if (sql.trim().toLowerCase().startsWith(INSERT_STR)) {
				insertSqlList.add(sql);
				continue;
			}
			throw new RuntimeException("illegal sql:" + sql);
		}
	}

	private List<String> splitSql(String multiSql) {
		multiSql = replaceFlagNotInQuoteExcludeNewline(multiSql, "/*", "*/", " ");
		multiSqlBeforeSplit = replaceFlagNotInQuoteExcludeNewline(multiSql, "--", "\n", " ");
		return splitIgnoreQuote(multiSqlBeforeSplit, ";".charAt(0));
	}


	private String replaceFlagNotInQuoteExcludeNewline(String sql, String startFlag, String endFlag, String replace) {

		boolean inQuotes = false;
		boolean inSingleQuotes = false;

		StringBuilder b = new StringBuilder();

		for (int i = 0; i < sql.length(); i++) {

			char c = sql.charAt(i);

			if (c == '\"' && !inSingleQuotes) {
				inQuotes = !inQuotes;
				b.append(c);
				continue;
			}
			if (c == '\'' && !inQuotes) {
				inSingleQuotes = !inSingleQuotes;
				b.append(c);
				continue;
			}

			// 在引号里面就直接过过过
			if (inQuotes || inSingleQuotes) {
				b.append(c);
				continue;
			}

			// 找到开端
			if (sql.startsWith(startFlag, i) && !sql.startsWith("/*+ ", i)) {
				boolean isFind = false;
				int localStart = i + startFlag.length();
				// 找不到结尾就一直过过过
				while (localStart++ < sql.length()) {
					if (sql.startsWith(endFlag, localStart)) {
						int end = localStart + endFlag.length();
						isFind = true;
						for (int j = i; j < end; j++) {
							if (sql.charAt(j) == '\n') {
								b.append('\n');
							} else {
								b.append(" ");
							}
						}
						i = end - 1;
						break;
					}
				}
				if (!isFind) {
					b.append(c);
				}
			} else {
				b.append(c);
			}
		}

		return b.toString();
	}

	private List<String> splitIgnoreQuote(String str, char delimiter) {
		List<String> tokensList = new ArrayList<>();
		boolean inQuotes = false;
		boolean inSingleQuotes = false;
		StringBuilder b = new StringBuilder();
		for (char c : str.toCharArray()) {
			if (c == delimiter) {
				if (inQuotes) {
					b.append(c);
				} else if (inSingleQuotes) {
					b.append(c);
				} else {
					tokensList.add(b.toString());
					b = new StringBuilder();
				}
			} else if (c == '\"' && !inSingleQuotes) {
				inQuotes = !inQuotes;
				b.append(c);
			} else if (c == '\'' && !inQuotes) {
				inSingleQuotes = !inSingleQuotes;
				b.append(c);
			} else {
				b.append(c);
			}
		}

		tokensList.add(b.toString());

		return tokensList;
	}

	/**
	 * 将kafka简化表转置为标准的源.库.表
	 * @param sql 原sql
	 * @return 转置后的sql
	 */
	private String kafkaTableConvert(String sql) {
		sql = sql + " ";
		//解析并解决kafka topic中带"."的情况，以及两段氏解析
		Matcher matcher = KAFKA_TABLE_PATTERN.matcher(sql);
		while (matcher.find()) {
			String[] split = matcher.group(0).replaceAll("`", "").trim().split("\\.");
			switch (split.length) {
				case 2:
					sql = sql.replaceAll(matcher.group(0), " Kafka_1.default_database." + split[1] + " ");
					break;
				case 3:
					boolean kafkaTableIsExist = keeperRpc.tableExist(split[0], "", split[1] + "." + split[2]);
					if (kafkaTableIsExist) {
						sql = sql.replaceAll(matcher.group(0), " Kafka_1.default_database.`" + split[1] + "." + split[2] + "` ");
					}
					break;
				case 4:
					sql = sql.replaceAll(matcher.group(0), " Kafka_1." + split[1] + ".`" + split[2] + "." + split[3] + "` ");						break;
			}
		}
		return sql;
	}

	public void addCreateTableSql(String createTableSql) {
		createTableSqlList.add(createTableSql);
	}

	public void addCreateViewSql(String createViewSql) {
		createViewSqlList.add(createViewSql);
	}

	public void addInsertSql(String insertSql) {
		insertSqlList.add(insertSql);
	}

	public void addCreateUdfSql(String createUdfSql) {
		createUdfSqlList.add(createUdfSql);
	}

	public String getOriginMultiSql() {
		return multiSqlBeforeSplit;
	}

	public List<String> getCreateTableSqlList() {
		return createTableSqlList;
	}

	public List<String> getCreateViewSqlList() {
		return createViewSqlList;
	}

	public List<String> getInsertSqlList() {
		return insertSqlList;
	}

	public List<String> getCreateUdfSqlList() {
		return createUdfSqlList;
	}


}
