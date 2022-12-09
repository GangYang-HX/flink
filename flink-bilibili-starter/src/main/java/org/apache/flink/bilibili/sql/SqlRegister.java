package org.apache.flink.bilibili.sql;

import org.apache.flink.bilibili.catalog.utils.MaterializeUtil;
import org.apache.flink.bilibili.udf.BuildInFunc;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

	private final StreamExecutionEnvironment env;
	private final StreamTableEnvironment tabEnv;
	private final StatementSet statementSet;

	public SqlRegister(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv, StatementSet statementSet) {
		this.env = env;
		this.tabEnv = tabEnv;
		this.statementSet = statementSet;
	}

	public void register(SqlTree tree,boolean explain) {
        registerCreateCatalog(tree.getCreateCatalogSqlList());
		registerCommonUdf();
		registerUserUdf(tree.getCreateUdfSqlList());
		registerCreateTable(tree.getCreateTableSqlList());
		registerCreateView(tree.getCreateViewSqlList());
		registerInsert(tree.getInsertSqlList());
        registerCreateMv(tree.getCreateMvSqlList(),explain);
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

	private void registerUserUdf(List<String> udfSqlList) {
		for (String udf : udfSqlList) {
			tabEnv.executeSql(udf);
		}
	}

	private void registerCreateTable(List<String> createSqlList) {
		for (String createSql : createSqlList) {
			tabEnv.executeSql(createSql);
			if (WK_PATTERN.matcher(createSql).find()) {
				env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			}
		}
	}

	private void registerCreateView(List<String> createViewList) {
		for (String createView : createViewList) {
			tabEnv.executeSql(createView);
		}
	}

	private void registerInsert(List<String> insertSqlList) {
		for (String insertSql : insertSqlList) {
			statementSet.addInsertSql(insertSql);
		}
	}
    private void registerCreateCatalog(List<String> createCatalogSqlList) {
        for (String createCatalogSql : createCatalogSqlList) {
            tabEnv.executeSql(createCatalogSql);
        }
    }
    private void registerCreateMv(List<String> createCatalogSqlList,boolean explain) {
        for(String sql : createCatalogSqlList){
            MaterializeUtil.convert(explain,env,tabEnv,statementSet,sql);
            tabEnv.executeSql(sql);
        }
    }
}
