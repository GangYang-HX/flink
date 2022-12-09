package com.bilibili.bsql.mysql.sharding;

import com.bilibili.bsql.common.global.SymbolsConstant;
import com.bilibili.bsql.common.utils.JDBCLock;
import com.bilibili.bsql.mysql.format.MysqlOutputFormat;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.ComplexShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className ShardingMysqlOutputFormat.java
 * @description This is the description of ShardingMysqlOutputFormat.java
 * @createTime 2020-10-22 15:08:00
 */
public class ShardingMysqlOutputFormat extends MysqlOutputFormat {
	private static final String DATA_SOURCE_BASENAME = "ds";

	public ShardingMysqlOutputFormat(DataType dataType, DynamicTableSink.Context context) {
		super(dataType, context);
	}

	@Override
	public void doOpen(int taskNumber, int numTasks) throws IOException {
		super.doOpen(taskNumber, numTasks);
		init();
	}

	@Override
	public String sinkType() {
		return "ShardingMySql";
	}

	public void init() throws IOException {
		try {
			establishConnection();
			upload = dbConn.prepareStatement(insertQuery);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("JDBC driver class not found.", e);
		}
	}

	@Override
	public Connection establishConnection() throws SQLException, ClassNotFoundException {
		synchronized (JDBCLock.class) {
			Class.forName(drivername);
		}
		// 配置真实数据源
		String[] shardingRealDBUrls = dbURL.split(SymbolsConstant.COMMA);
		Map<String, DataSource> dataSourceMap = new HashMap<>(shardingRealDBUrls.length);
		int dataSourceCount = 0;

		List<String> dbList = new ArrayList<>();
		for (String shardingRealDBUrl : shardingRealDBUrls) {
			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName("com.mysql.jdbc.Driver");
			dataSource.setUrl(shardingRealDBUrl);
			if (username != null) {
				dataSource.setUsername(username);
				dataSource.setPassword(password);
			}
			dataSourceMap.put(DATA_SOURCE_BASENAME + dataSourceCount, dataSource);
			dbList.add(DATA_SOURCE_BASENAME + dataSourceCount);
			dataSourceCount++;
		}

		String[] realTableNameArray = realTableNames.split(SymbolsConstant.COMMA);
		StringBuilder actualDataNodes = new StringBuilder();
		for (int i = 0; i < realTableNameArray.length; i++) {
			actualDataNodes.append("ds${0.." + (dataSourceCount - 1) + "}." + realTableNameArray[i]);
			if (i != realTableNameArray.length - 1) {
				actualDataNodes.append(',');
			}
		}

		// 配置Order表规则（logic table，realTables）
		TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration(tableName, actualDataNodes.toString());
		// table sharding strategy
		Tuple3<Method, String, Class> tableTuple = parseShardingMethodAndColumns(targetTables);
		orderTableRuleConfig.setTableShardingStrategyConfig(
			new ComplexShardingStrategyConfiguration(
				tableTuple.f1.substring(tableTuple.f1.indexOf(",") + 1),
				new AdminIdShardingAlgorithm(tableTuple, realTableNameArray)
			)
		);

		// db sharding strategy
		Tuple3<Method, String, Class> dbTuple = parseShardingMethodAndColumns(targetDBs);
		orderTableRuleConfig.setDatabaseShardingStrategyConfig(
			new ComplexShardingStrategyConfiguration(dbTuple.f1.substring(dbTuple.f1.indexOf(",") + 1),
				new AdminIdShardingAlgorithm(dbTuple, dbList.toArray(new String[dbList.size()]))
			)
		);
		// 配置分片规则
		ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
		shardingRuleConfig.getTableRuleConfigs().add(orderTableRuleConfig);
		// 获取数据源对象
		DataSource dataSource = ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, defaultConnectionConfig);
		dbConn = dataSource.getConnection();
		return dbConn;
	}

	private Tuple3<Method, String, Class> parseShardingMethodAndColumns(String shardingFunctionStr) {
		Tuple3<Method, String, Class> ret = new Tuple3<>();
		Class shardingClass = (Class) (getDdlUDFMap().get(shardingFunctionStr.split("\\(")[0]));
		if (shardingClass == null) {
			throw new IllegalArgumentException("no match sharding class");
		}
		int evalMethodCount = 0;
		Method evalMethod = null;
		for (Method method : shardingClass.getMethods()) {
			if ("eval".equals(method.getName())) {
				evalMethod = method;
				evalMethodCount++;
			}
		}
		if (evalMethodCount != 1) {
			throw new IllegalArgumentException("the sharding class don't have unique eval method");
		}
		if (!evalMethod.getReturnType().isAssignableFrom(List.class)) {
			throw new IllegalArgumentException("the sharding class 's eval method don't have List return type");
		}
		ret.f0 = evalMethod;
		ret.f1 = shardingFunctionStr.split("\\(")[1].replaceAll("\\)", "").trim();
		try {
			Integer.parseInt(ret.f1.substring(0, ret.f1.indexOf(",")));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("the input columns are illegal");
		}
		HashSet<String> inputColumnSet = new HashSet<>(Arrays.asList(ret.f1.substring(ret.f1.indexOf(",") + 1).split(",")));
		HashSet<String> fieldSet = new HashSet<>(Arrays.asList(getDbSink().getFieldNames()));
		if (!fieldSet.containsAll(inputColumnSet)) {
			throw new IllegalArgumentException("the input columns are illegal");
		}
		ret.f2 = shardingClass;
		return ret;
	}
}
