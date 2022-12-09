package com.bilibili.bsql.clickhouse.shard.table;

import com.bilibili.bsql.clickhouse.shard.format.ClickHouseShardOutputFormat;
import com.bilibili.bsql.clickhouse.shard.tableinfo.ClickHouseShardTableInfo;
import com.bilibili.bsql.common.failover.FailOverConfigInfo;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

/**
 * @author: zhuzhengjun
 * @date: 2021/2/19 10:55 上午
 */
public class BsqlClickHouseShardDynamicSink implements DynamicTableSink {


	public ClickHouseShardTableInfo clickHouseShardTableInfo;

	protected int[] sqlTypes;
	public static final int CLICK_INT_ARRAY = 3001;
	public static final int CLICK_FLOAT_ARRAY = 3002;
	public static final int CLICK_STRING_ARRAY = 3003;
	public static final int CLICK_STRING_STRING_MAP = 3004;

	public BsqlClickHouseShardDynamicSink(ClickHouseShardTableInfo clickHouseShardTableInfo) {
		this.clickHouseShardTableInfo = clickHouseShardTableInfo;
	}


	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		// UPSERT mode
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}


	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		List<LogicalType> fieldTypeArray = Arrays.asList(clickHouseShardTableInfo.getFieldLogicalType());
		buildSqlTypes(fieldTypeArray);
		ClickHouseShardOutputFormat clickHouseShardOutputFormat = new ClickHouseShardOutputFormat(clickHouseShardTableInfo, clickHouseShardTableInfo.getPhysicalRowDataType(), context, Arrays.asList(clickHouseShardTableInfo.getFieldNames()), sqlTypes, fieldTypeArray);
		FailOverConfigInfo failOverConfigInfo = FailOverConfigInfo.FailOverConfigInfoBuilder.aFailOverConfigInfo()
			.withBaseFailBackInterval(clickHouseShardTableInfo.getBaseFailBackInterval())
			.withFailOverRate(clickHouseShardTableInfo.getFailOverRate())
			.withFailOverSwitchOn(clickHouseShardTableInfo.getFailOverSwitchOn())
			.withFailOverTimeLength(clickHouseShardTableInfo.getFailOverTimeLength())
			.withMaxFailedIndexRatio(clickHouseShardTableInfo.getMaxFailedIndexRatio())
			.build();
		clickHouseShardOutputFormat.setFailOverConfigInfo(failOverConfigInfo);
		clickHouseShardOutputFormat.setBatchMaxCount(clickHouseShardTableInfo.getBatchMaxCount());
		clickHouseShardOutputFormat.setBatchMaxTimeout(clickHouseShardTableInfo.getBatchMaxTimeout());
		return SinkFunctionProviderWithParallel.of(clickHouseShardOutputFormat, clickHouseShardTableInfo.getParallelism());
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlClickHouseShardDynamicSink(clickHouseShardTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "ClickHouse-shard-table-sink";
	}

	/**
	 * By now specified class type conversion.
	 * FIXME Follow-up has added a new type of time needs to be modified
	 * // TODO Need to verify type mapping
	 *
	 * @param fieldTypeArray
	 */
	protected void buildSqlTypes(List<LogicalType> fieldTypeArray) {
		int[] tmpFieldsType = new int[fieldTypeArray.size()];
		for (int i = 0; i < fieldTypeArray.size(); i++) {
			LogicalType logicalType = fieldTypeArray.get(i);
			String fieldType = logicalType.getClass().getName();
			if (fieldType.equals(IntType.class.getName())) {
				tmpFieldsType[i] = Types.INTEGER;
			} else if (fieldType.equals(BooleanType.class.getName())) {
				tmpFieldsType[i] = Types.BOOLEAN;
			} else if (fieldType.equals(BigIntType.class.getName())) {
				tmpFieldsType[i] = Types.BIGINT;
			} else if (fieldType.equals(TinyIntType.class.getName())) {
				tmpFieldsType[i] = Types.TINYINT;
			} else if (fieldType.equals(SmallIntType.class.getName())) {
				tmpFieldsType[i] = Types.SMALLINT;
			} else if (fieldType.equals(VarCharType.class.getName())) {
				tmpFieldsType[i] = Types.CHAR;
			} else if (fieldType.equals(BinaryType.class.getName())) {
				tmpFieldsType[i] = Types.BINARY;
			} else if (fieldType.equals(FloatType.class.getName())) {
				tmpFieldsType[i] = Types.FLOAT;
			} else if (fieldType.equals(DoubleType.class.getName())) {
				tmpFieldsType[i] = Types.DOUBLE;
			} else if (fieldType.equals(TimestampType.class.getName())) {
				tmpFieldsType[i] = Types.TIMESTAMP;
			} else if (fieldType.equals(DecimalType.class.getName())) {
				tmpFieldsType[i] = Types.DECIMAL;
			} else if (fieldType.equals(DateType.class.getName())) {
				tmpFieldsType[i] = Types.DATE;
			} else if (fieldType.equals(ArrayType.class.getName())) {
				List<LogicalType> child = logicalType.getChildren();
				for (LogicalType childType : child) {
					if (childType.getClass().getName().equals(IntType.class.getName())) {
						tmpFieldsType[i] = CLICK_INT_ARRAY;
						break;
					} else if (childType.getClass().getName().equals(FloatType.class.getName())) {
						tmpFieldsType[i] = CLICK_FLOAT_ARRAY;
						break;
					} else if (childType.getClass().getName().equals(VarCharType.class.getName())) {
						tmpFieldsType[i] = CLICK_STRING_ARRAY;
						break;
					}
				}
			} else if (fieldType.equals(MapType.class.getName())) {
				tmpFieldsType[i] = CLICK_STRING_STRING_MAP;
			} else if (fieldType.equals(LocalZonedTimestampType.class.getName())) {
				tmpFieldsType[i] = Types.TIMESTAMP;
			} else {
				throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
			}
		}
		this.sqlTypes = tmpFieldsType;
	}
}


