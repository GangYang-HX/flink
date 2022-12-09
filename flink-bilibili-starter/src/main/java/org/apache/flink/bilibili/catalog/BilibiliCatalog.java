package org.apache.flink.bilibili.catalog;

import org.apache.flink.bilibili.catalog.utils.CatalogUtil;
import org.apache.flink.bilibili.catalog.utils.HoodieUtils;
import org.apache.flink.bilibili.enums.DsOpTypeEnum;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;

import com.bapis.datacenter.service.keeper.*;
import io.grpc.StatusRuntimeException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pleiades.component.ecode.ServerCode;
import pleiades.component.rpc.core.StatusCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BilibiliCatalog extends AbstractBilibiliCatalog {

	private final static Logger LOG = LoggerFactory.getLogger(BilibiliCatalog.class);

	public static final String DEFAULT_DB = "default";
	public static final String DEFAULT_DS = "default";
	//private static final KeeperRpc keeperRpc;
	private static final KeeperConnection keeperConnection;
	private String currentDataSource;

	static {
        keeperConnection = KeeperConnection.getInstance();
	}

	public BilibiliCatalog(String name) {
		this(name, DEFAULT_DS, DEFAULT_DB);
	}

	public BilibiliCatalog(String name, String defaultDataSource, String defaultDatabase) {
		super(name, defaultDataSource, defaultDatabase);
	}

	@Override
	public void open() {
		//keeperRpc.open();
		LOG.info("Connected to keeper metastore.");
	}

	@Override
	public void close() {
		if (keeperConnection != null) {
            keeperConnection.close();
			LOG.info("Close connection to keeper metastore");
		}
	}

	// ------ dataSources ------
    @Override
	public void setCurrentDataSource(String dataSource) throws CatalogException {
		this.currentDataSource = dataSource;
	}

	// ------ databases ------

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		//todo 待keeper完善
		return true;
	}

	// ------ tables ------

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws CatalogException {
		try {
			TableDto tableDto = keeperConnection.getTable(currentDataSource, tablePath.getDatabaseName(), tablePath.getObjectName());
            return instantiateCommonCatalogTable(tableDto);
		} catch (Exception e) {
			extractKeeperEx(e);
			throw new CatalogException(
				String.format("Failed getting table %s", tablePath.getFullName()), e);
		}
	}

    private CatalogBaseTable instantiateCommonCatalogTable(TableDto tableDto) {
        TableSchema.Builder builder = TableSchema.builder();
        List<ColumnDto> columnDtos = tableDto.getColumnsList();
        if (CollectionUtils.isNotEmpty(columnDtos)) {
            for (ColumnDto columnDto : columnDtos) {
                if (columnDto.getIsPartition()) {
                    continue;
                }
                TableColumn tableColumn = TableColumn.of(columnDto.getColName(), convertDataType(columnDto, tableDto.getDatabase().getDbType()));
                builder.add(tableColumn);
            }
        }

        //连接属性
        Map<String, String> props = constructTableProps(tableDto);

        return new CatalogTableImpl(builder.build(), props, "");
    }
	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return keeperConnection.tableExist(currentDataSource, tablePath.getDatabaseName(), tablePath.getObjectName());
	}

	/**
	 * keeper报错信息解析
	 *
	 * @param e
	 */
	private void extractKeeperEx(Exception e) {
		if (e instanceof StatusRuntimeException) {
			try {
				StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
				ServerCode serverCode = StatusCode.toServerCode(statusRuntimeException.getStatus(), statusRuntimeException.getTrailers());
				LOG.error("KeeperService调用失败，错误码：{},错误信息：{}", serverCode.getCode(), serverCode.getMessage());
			} catch (Exception ex) {
				LOG.error("extract keeper ex error:", ex);
			}
		}
	}

	/**
	 * keeper type --> flink data type
	 *
	 * @param columnDto
	 * @return
	 */
	public DataType convertDataType(ColumnDto columnDto, DbType dbType) {
		switch (dbType) {
			case Hive:
				return convertHiveDataType(columnDto);
			case Kafka:
				return convertKafkaDataType(columnDto);
			case Clickhouse:
				return convertClickhouseDataType(columnDto);
			case Mysql:
			case TiDB:
				return convertMysqlDataType(columnDto);
			default:
				return convertDataType(columnDto.getMappingColTypeEnum(), columnDto.getColType());
		}
	}

	/**
	 * 转换hive数据类型
	 * @param columnDto
	 * @return
	 */
	private DataType convertHiveDataType(ColumnDto columnDto) {
		String columnType = columnDto.getColType().toLowerCase().trim();
		if ("binary".equals(columnType)) {
			return DataTypes.VARBINARY(Integer.MAX_VALUE);
		} else {
			if (columnType.startsWith("array")) {
				String patternType = columnType.substring(columnType.indexOf("<") + 1, columnType.length() - 1);
				return DataTypes.ARRAY(convertPatternType(patternType));
			}
			if (columnType.startsWith("map")) {
				String[] patternTypes = columnType.substring(columnType.indexOf("<") + 1, columnType.length() - 1).split(",");
				return DataTypes.MAP(convertPatternType(patternTypes[0]), convertPatternType(patternTypes[1]));
			}
		}

		return convertDataType(columnDto.getMappingColTypeEnum(), columnDto.getColType());
	}

	/**
	 * 转换kafka数据类型
	 * @param columnDto
	 * @return
	 */
	private DataType convertKafkaDataType(ColumnDto columnDto) {
		String columnType = columnDto.getColType();
		if (columnType.startsWith("ARRAY")) {
			String patternType = columnType.toLowerCase().substring(columnType.indexOf("<") + 1, columnType.length() - 1).toLowerCase();
			return DataTypes.ARRAY(convertPatternType(patternType));
		}
		if (columnType.startsWith("MAP")) {
			String[] patternTypes = columnType.toLowerCase().substring(columnType.indexOf("<") + 1, columnType.length() - 1).toLowerCase().split(",");
			return DataTypes.MAP(convertPatternType(patternTypes[0]), convertPatternType(patternTypes[1]));
		}
		return convertDataType(columnDto.getMappingColTypeEnum(), columnDto.getColType());
	}

	/**
	 * 转换clickhouse数据类型
	 * @param columnDto
	 * @return
	 */
	private DataType convertClickhouseDataType(ColumnDto columnDto) {

		DataType dataType = convertClickhouseDataType(columnDto.getColType());
		if (Objects.nonNull(dataType)) {
			return dataType;
		}

		//Array
		if (columnDto.getColType().startsWith("Array")) {
			String elementDataTypeStr = columnDto.getColType()
				.substring(columnDto.getColType().indexOf("(") + 1, columnDto.getColType().indexOf(")"));
			DataType elementDataType = convertClickhouseDataType(elementDataTypeStr);
			if (Objects.nonNull(elementDataType)) {
				return DataTypes.ARRAY(elementDataType);
			}
		}

		//Map
		if (columnDto.getColType().startsWith("Map")) {
			String keyedElementDataTypeStr = columnDto.getColType()
				.substring(columnDto.getColType().indexOf("(") + 1, columnDto.getColType().indexOf(")"));
			DataType elementKeyDataType = convertClickhouseDataType(keyedElementDataTypeStr.split(",")[0]);
			DataType elementValueDataType = convertClickhouseDataType(keyedElementDataTypeStr.split(",")[1]);
			if (Objects.nonNull(elementKeyDataType) && Objects.nonNull(elementValueDataType)) {
				return DataTypes.MAP(elementKeyDataType, elementValueDataType);
			}
		}

		return convertDataType(columnDto.getMappingColTypeEnum(), columnDto.getColType());
	}

	private DataType convertClickhouseDataType(String colType) {
		switch (colType) {
			case "Int8":
			case "Int16":
			case "UInt8":
			case "UInt16":
				return DataTypes.INT();
			case "Int32":
			case "Int64":
			case "UInt32":
			case "UInt64":
				return DataTypes.BIGINT();
			case "Float32":
			case "Float64":
				return DataTypes.DOUBLE();
			case "Decimal32":
				return DataTypes.DECIMAL(9, 4);
			case "Decimal64":
				return DataTypes.DECIMAL(18, 4);
			case "Decimal128":
				return DataTypes.DECIMAL(38, 4);
			case "Datetime":
			case "DateTime":
				return DataTypes.TIMESTAMP();
			case "String":
				return DataTypes.STRING();
			case "Array":
				return DataTypes.ARRAY(DataTypes.STRING());
			default:
				return null;
		}
	}


	/**
	 * 转换mysql/tidb数据类型
	 * @param columnDto
	 * @return
	 */
	private DataType convertMysqlDataType(ColumnDto columnDto) {
		String colType = columnDto.getColType().toLowerCase();
		switch (colType) {
			case "smallint":
			case "int":
			case "integer":
				return DataTypes.INT();
			case "tinyint":
				return DataTypes.BOOLEAN();
			case "bigint":
				return DataTypes.BIGINT();
			case "float":
				return DataTypes.FLOAT();
			case "double":
				return DataTypes.DOUBLE();
			case "date":
				return DataTypes.DATE();
			case "time":
				return DataTypes.TIME();
			case "datetime":
			case "timestamp":
				return DataTypes.TIMESTAMP();
			case "char":
			case "varchar":
			case "text":
				return DataTypes.STRING();
			default:
				if (colType.startsWith("decimal")) {
					String[] split = columnDto.getColType().toLowerCase().substring(colType.indexOf("(") + 1, colType.length() - 1).split(",");
					return DataTypes.DECIMAL(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
				}
				return convertDataType(columnDto.getMappingColTypeEnum(), columnDto.getColType());
		}
	}


	/**
	 * 默认转换
	 * @param unitedColTypeEnum
	 * @param columnType
	 * @return
	 */
	private DataType convertDataType(UnitedColTypeEnum unitedColTypeEnum, String columnType) {
		switch (unitedColTypeEnum) {
			case FLOAT:
				return DataTypes.FLOAT();
			case BYTES:
				return DataTypes.BYTES();
			case TIME:
				return DataTypes.TIME();
			case DATE:
				return DataTypes.DATE();
			case INT:
				return DataTypes.INT();
			case BIGINT:
				return DataTypes.BIGINT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case STRING:
				return DataTypes.STRING();
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case DECIMAL:
				String[] decimalPrecision = columnType.toLowerCase().substring(columnType.indexOf("(") + 1, columnType.length() - 1).split(",");
				return DataTypes.DECIMAL(Integer.parseInt(decimalPrecision[0]), Integer.parseInt(decimalPrecision[1]));
			case TINYINT:
				return DataTypes.TINYINT();
			case SMALLINT:
				return DataTypes.SMALLINT();
			case TIMESTAMP:
				return DataTypes.TIMESTAMP();
			default:
				throw new UnsupportedOperationException(
					String.format("Doesn't support col type '%s' or '%s' yet", unitedColTypeEnum, columnType));
		}
	}

	/**
	 *
	 * @param patternType
	 * @return
	 */
	private DataType convertPatternType(String patternType) {
		switch (patternType) {
			case "string":
				return DataTypes.STRING();
			case "bigint":
				return DataTypes.BIGINT();
			case "double":
				return DataTypes.DOUBLE();
			case "binary":
				return DataTypes.VARBINARY(Integer.MAX_VALUE);
			default:
				if (patternType.startsWith("decimal")) {
					String[] split = patternType.substring(patternType.indexOf("(") + 1, patternType.length() - 1).split(",");
					return DataTypes.DECIMAL(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
				}
				throw new UnsupportedOperationException(
					String.format("Doesn't support pattern type '%s' yet", patternType));
		}
	}


	private Map<String, String> constructTableProps(TableDto tableDto) {
		DbType dbType = tableDto.getDatabase().getDbType();
		Map<String, String> propsMap = new HashMap<>();
		switch (dbType) {
			case Hive:
                return CatalogUtil.constructHiveTablePropsMap(tableDto);
			case Kafka:
                //TODO 由于bili kafka-connector还未迁移，暂先使用官方版本
			    //bsql-kafka10
//				propsMap.put("connector", "bsql-kafka10");
//				propsMap.put("bootstrapServers", tableDto.getDatabase().getPropertiesMap().get("bootstrapServers"));
//				propsMap.put("topic", StringUtils.defaultString(tableDto.getTableName()));
//				propsMap.put("bsql-delimit.delimiterKey", tableDto.getPropertiesMap().get("colSeparator"));
                //kafka
                propsMap.put("connector", "kafka");
                propsMap.put("properties.bootstrap.servers", tableDto.getDatabase().getPropertiesMap().get("bootstrapServers"));
                propsMap.put("topic", StringUtils.defaultString(tableDto.getTableName()));
                propsMap.put("format", "csv");
                propsMap.put("csv.field-delimiter", tableDto.getPropertiesMap().get("colSeparator"));
                propsMap.put("csv.ignore-parse-errors", "true");
                return propsMap;
			case Clickhouse:
				//获取数据源的写配置信息
				DatasourceConfig clickhouseConfigs = keeperConnection.getDsConfigsByDsIdAndUser(tableDto.getDatabase().getDsId(), DbType.Clickhouse, DsOpTypeEnum.WRITE);
				propsMap.put("connector", "bsql-clickhouse-shard");
				propsMap.put("databaseName", tableDto.getDatabase().getDatabaseName());
				propsMap.put("tableName", tableDto.getTableName());
				propsMap.put("batchSize", "5000");
				propsMap.put("batchMaxTimeout", "300000"); //olap那边要调大该默认参数，如果用户有需求可自行覆盖
				propsMap.put("userName", clickhouseConfigs.getUser());
				propsMap.put("password", clickhouseConfigs.getPassword());
				propsMap.put("url", "clickhouse://" + clickhouseConfigs.getHost() + ":" + clickhouseConfigs.getPort());
				return propsMap;
			case Redis:
				propsMap.put("connector", "bsql-redis");
				propsMap.put("redisType", "3");
				propsMap.put("url", tableDto.getDatabase().getPropertiesMap().get("servers"));
				return propsMap;
			case Kfc:
				propsMap.put("connector", "bsql-kfc");
				propsMap.put("url", tableDto.getDatabase().getPropertiesMap().get("address"));
				return propsMap;
			case Databus:
				propsMap.put("connector", "bsql-databus");
				propsMap.put("address", tableDto.getDatabase().getPropertiesMap().get("address"));
				propsMap.put("appKey", tableDto.getDatabase().getPropertiesMap().get("appkey"));
				propsMap.put("appSecret", tableDto.getDatabase().getPropertiesMap().get("secret"));
				propsMap.put("topic", tableDto.getTableName());
				propsMap.put("bsql-delimit.delimiterKey", "\\|");
				return propsMap;
			case Mysql:
			case TiDB:
				//获取数据源的写配置信息
				DatasourceConfig mysqlConfigs = keeperConnection.getDsConfigsByDsIdAndUser(tableDto.getDatabase().getDsId(), dbType, DsOpTypeEnum.WRITE);
				String host = mysqlConfigs.getHost();
				String port = mysqlConfigs.getPort();
				propsMap.put("connector", "bsql-mysql");
				propsMap.put("url", "jdbc:mysql://" + host + ":" + port + "/" + tableDto.getDatabase().getDatabaseName() + "?characterEncoding=utf8");
				propsMap.put("tableName", tableDto.getTableName());
				propsMap.put("userName", mysqlConfigs.getUser());
				propsMap.put("password", mysqlConfigs.getPassword());
				propsMap.put("connectionPoolSize", "1");
				propsMap.put("batchMaxTimeout", "10000");
				propsMap.put("batchSize", "1000");
				return propsMap;
			default:
				throw new UnsupportedOperationException(
					String.format("Doesn't support catalog '%s' yet", dbType));
		}
	}

}
