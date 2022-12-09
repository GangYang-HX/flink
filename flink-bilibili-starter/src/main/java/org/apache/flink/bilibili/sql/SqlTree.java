package org.apache.flink.bilibili.sql;


import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


/**
 * @author galaxy
 */
public class SqlTree {

    private static final String CREATE_UDF_STR = "(?i)\\s*create\\s+function\\s+(\\S+)\\s+as\\s+(\\S+)\\sLANGUAGE\\s+(\\S+)";
    private static final Pattern CREATE_UDF_PATTERN = Pattern.compile(CREATE_UDF_STR);

    private static final String CREATE_TABLE_STR = "(?i)create\\s+table\\s+(\\S+)\\s*(\\((.+)\\))?\\s*(with\\s*\\((.+)\\))?";
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(CREATE_TABLE_STR);

    private static final String CREATE_CATALOG_STR =
            "(?i)create\\s+catalog\\s+(\\S+)\\s*(\\((.+)\\))?\\s*(with\\s*\\((.+)\\))?";
    private static final Pattern CREATE_CATALOG_PATTERN = Pattern.compile(CREATE_CATALOG_STR);


    private static final String CREATE_VIEW_STR = "(?i)create\\s+view\\s+([^\\s]+)\\s+as\\s+select\\s+(.*)";
    private static final Pattern CREATE_VIEW_PATTERN = Pattern.compile(CREATE_VIEW_STR);

    private static final String CREATE_MV_STR = "(?i)create\\s+materialized\\s++view\\s+(\\S+)\\s*(\\((.+)\\))?\\s*(with\\s*\\((.+)\\))?\\s+as\\s+select\\s+(.*)";
    private static final Pattern CREATE_MV_PATTERN = Pattern.compile(CREATE_MV_STR);

    private static final String INSERT_STR = "insert";

    private final List<String> createTableSqlList;
    private final List<String> createCatalogSqlList;
    private final List<String> createViewSqlList;
    private final List<String> insertSqlList;
    private final List<String> createUdfSqlList;
    private final List<String> createMvSqlList;

    public SqlTree(String multiSql) {

        List<String> sqlList = splitSql(multiSql);
        createTableSqlList = new ArrayList<>();
        createCatalogSqlList = new ArrayList<>();
        createViewSqlList = new ArrayList<>();
        insertSqlList = new ArrayList<>();
        createUdfSqlList = new ArrayList<>();
        createMvSqlList = new ArrayList<>();

        for (String sql : sqlList) {
            sql = sql.trim();
            if (StringUtils.isBlank(sql)) {
                continue;
            }
            if (CREATE_UDF_PATTERN.matcher(sql).find()) {
                createUdfSqlList.add(sql);
                continue;
            }

            if (CREATE_TABLE_PATTERN.matcher(sql).find()) {
                createTableSqlList.add(sql);
                continue;
            }
            if (CREATE_CATALOG_PATTERN.matcher(sql).find()) {
                createCatalogSqlList.add(sql);
                continue;
            }
            if (CREATE_VIEW_PATTERN.matcher(sql).find()) {
                createViewSqlList.add(sql);
                continue;
            }
            if (CREATE_MV_PATTERN.matcher(sql).find()) {
                createMvSqlList.add(sql);
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

        String newSql = replaceFlagNotInQuoteExcludeNewline(multiSql, "/*", "*/", " ");
        newSql = replaceFlagNotInQuoteExcludeNewline(multiSql, "--", "\n", " ");
        return splitIgnoreQuote(newSql, ";".charAt(0));
    }

    private String replaceIgnoreQuote(String sql, String flag, String newFlag) {

        boolean inQuotes = false;
        boolean inSingleQuotes = false;

        StringBuilder b = new StringBuilder();

        for (int i = 0; i < sql.length(); i++) {

            char c = sql.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes;
                b.append(c);
                continue;
            }
            if (c == '\'') {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
                continue;
            }

            // 在引号里面就直接过过过
            if (inQuotes || inSingleQuotes) {
                b.append(c);
                continue;
            }
            String sub = sql.substring(i, Math.min(i + flag.length(), sql.length()));
            if (sub.equals(flag)) {
                b.append(newFlag);
                i += flag.length() - 1;
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }


    private String replaceFlagNotInQuoteExcludeNewline(String sql, String startFlag, String endFlag, String replace) {

        boolean inQuotes = false;
        boolean inSingleQuotes = false;

        StringBuilder b = new StringBuilder();

        for (int i = 0; i < sql.length(); i++) {

            char c = sql.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes;
                b.append(c);
                continue;
            }
            if (c == '\'') {
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
            } else if (c == '\"') {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'') {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else {
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
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

    public List<String> getCreateMvSqlList() {
        return createMvSqlList;
    }
    public List<String> getCreateCatalogSqlList() {
        return createCatalogSqlList;
    }

}
