package com.bilibili.bsql.hive.tableinfo;

import com.bilibili.bsql.common.TableInfo;
import com.bilibili.bsql.hive.filetype.parquet.SchemaUtil;
import com.bilibili.bsql.hive.utils.InvokeUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_PARALLELISM;
import static com.bilibili.bsql.hive.tableinfo.HiveConfig.*;
import static com.bilibili.bsql.hive.tableinfo.HiveConfig.BSQL_CUSTOM_TRACE_CLASS;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/6 7:31 下午
 */
@Data
public class HiveSinkTableInfo implements Serializable {
	public static final Logger LOG = LoggerFactory.getLogger(HiveSinkTableInfo.class);



	protected final String compress;
	protected final String bufferSize;
	protected final String batchSize;
	protected final String orcRowsBetweenMemoryChecks;

	protected final String timeField;
	protected final boolean timeFieldVirtual;
	protected final String metaField;
	protected final String bucketTimeKey;
	protected String type = "default";
	protected String jobId;
	/**
	 * this variable include computed part
	 */
	protected final List<TableInfo.FieldInfo> fields;
	/**
	 * this variable exclude computed part
	 */
	protected final List<TableInfo.FieldInfo> physicalFields;

	/**
	 * because TableColumn is not serializable, so we transform it to the FieldInfo
	 * the rawFields is not accessible in the runtime
	 */
	protected transient List<TableColumn> rawFields;
	protected final Map<String, String> props;
	protected final TypeInformation<?>[] typeInformations;
	protected final DataType dataType;
	protected final TableSchema tableSchema;
	protected final String parallelism;
	protected Integer eventTimePos = -1;
	protected Integer metaPos = -1;
	protected ReadableConfig readableConfig;
	protected ObjectIdentifier objectIdentifier;
	protected ArrayList<String> partitionKeyList;
	protected String sinkHiveVersion;
	protected String partitionUdf;
	protected String partitionUdfClass;
	protected Class<?> multiPartitionUdfClass;
	protected int[] multiPartitionFieldIndex;
	protected boolean eagerCommit;
	protected final boolean allowDrift;
	protected final boolean allowEmptyCommit;
	protected final boolean pathContainPartitionKey;
	private final boolean enableIndexFile;
	protected final String traceId;
	protected final String traceKind;
	protected final String customTraceClass;
	protected String commitPolicy;
	protected String customPolicyReferences;
	protected final String metastoreCommitLimit;
	protected final boolean defaultPartitionCheck;
	protected final boolean multiPartitionEmptyCommit;
	protected final Long switchTime;
	protected final boolean partitionRollback;
	protected final boolean useTimestampFilePrefix;
	protected final Long rollingPolicyFileSize;
	protected final Long rollingPolicyTimeInterval;
	protected final String commitDelay;


	public HiveSinkTableInfo(TableSinkFactory.Context context) {
		this.rawFields = context.getTable().getSchema().getTableColumns();
		this.fields = rawFields.stream().map(TableInfo::fromColumn).collect(Collectors.toList());
		this.physicalFields = fields.stream().filter(column -> !column.isGenerated()).collect(Collectors.toList());
		this.props = context.getTable().getOptions();

		this.compress = props.getOrDefault(BSQL_HIVE_COMPRESS.key(),"");
		this.bufferSize = props.get(BSQL_HIVE_BUFFER_SIZE.key());
		this.batchSize = props.get(BSQL_HIVE_BATCH_SIZE.key());
		this.orcRowsBetweenMemoryChecks = props.get(BSQL_ORC_ROWS_BETWEEN_MEMORY_CHECKS.key());


		this.parallelism = props.get(BSQL_PARALLELISM.key());
		this.typeInformations = getTypeInformation(getFieldClass());

		this.readableConfig = context.getConfiguration();
		this.sinkHiveVersion = props.get(BSQL_HIVE_VERSION.key()) == null ? "2.3.4" : props.get(BSQL_HIVE_VERSION.key());
		this.partitionUdf = props.get(BSQL_HIVE_PARTITION_UDF.key());
		this.partitionUdfClass = props.get(BSQL_HIVE_PARTITION_UDF_CLASS.key());
		this.timeField = props.get(BSQL_HIVE_TIME_FIELD.key());
		this.metaField = props.get(BSQL_HIVE_META_FIELD.key());
		this.bucketTimeKey = props.get(BSQL_HIVE_BUCKET_TIME_KEY.key());
		this.timeFieldVirtual = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_TIME_FIELD_VIRTUAL.key(), "false"));
		String[] fieldNames = getFieldNames();
		if (null != timeField) {
			for (int i = 0; i < fieldNames.length; i++) {
				if (timeField.equals(fieldNames[i])) {
					eventTimePos = i;
					break;
				}
			}
		}
		if (null != metaField) {
			for (int i = 0; i < fieldNames.length; i++) {
				if (metaField.equals(fieldNames[i])) {
					metaPos = i;
					break;
				}
			}
		}
		this.tableSchema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
		this.dataType = TableSchemaUtils.getDataType(tableSchema);
		if (useMultiPartitionUdf()) {
			try {
				this.initMultiPartitionUdf();
			} catch (Exception e) {
				checkArgument(false, String.format("init partition udf class error %s", e));
			}
		}
		this.eagerCommit = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_EAGERLY_COMMIT.key(), "false"));
		this.commitPolicy = props.get(BSQL_COMMIT_POLICY.key());
		this.customPolicyReferences = props.get(BSQL_CUSTOM_POLICY_CLASS.key());
		this.allowDrift = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_ALLOW_DRIFT.key(), "false"));
		this.allowEmptyCommit = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_ALLOW_EMPTY_COMMIT.key(), "false"));
		this.pathContainPartitionKey = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_PATH_CONTAIN_PARTITION_KEY.key(), "true"));
		this.enableIndexFile = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_ENABLE_INDEX_FILE.key(), "false"));
		this.traceId = props.getOrDefault(BSQL_HIVE_TRACE_ID.key(), "");
		this.traceKind = props.getOrDefault(BSQL_HIVE_TRACE_KIND.key(), "");
		this.customTraceClass = props.getOrDefault(BSQL_CUSTOM_TRACE_CLASS.key(), BSQL_CUSTOM_TRACE_CLASS.defaultValue());
		this.metastoreCommitLimit = props.getOrDefault(BSQL_METASTORE_COMMIT_LIMIT.key(), BSQL_METASTORE_COMMIT_LIMIT.defaultValue());
		this.defaultPartitionCheck = Boolean.parseBoolean(props.getOrDefault(BSQL_DEFAULT_PARTITION_CHECK.key(), "false"));
		this.switchTime = Long.parseLong(props.getOrDefault(BSQL_HIVE_SWITCH_TIME.key(),"0"));
		this.partitionRollback = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_PARTITION_ROLLBACK.key(), "false"));
		this.multiPartitionEmptyCommit =
			Boolean.parseBoolean(props.getOrDefault(BSQL_MULTI_PARTITION_EMPTY_COMMIT.key(), "false"));
		this.useTimestampFilePrefix = Boolean.parseBoolean(props.getOrDefault(BSQL_HIVE_USE_TIMESTAMP_FILE_PREFIX.key(), "false"));
		this.rollingPolicyFileSize = Long.parseLong(props.getOrDefault(BSQL_SINK_ROLLING_POLICY_FILE_SIZE.key(), String.valueOf(BSQL_SINK_ROLLING_POLICY_FILE_SIZE.defaultValue())));
		this.rollingPolicyTimeInterval = Long.parseLong(props.getOrDefault(BSQL_SINK_ROLLING_POLICY_TIME_INTERVAL.key(), String.valueOf(BSQL_SINK_ROLLING_POLICY_TIME_INTERVAL.defaultValue())));
		this.commitDelay = props.getOrDefault(BSQL_HIVE_COMMIT_DELAY_MS.key(), "600000");
    }

	protected void initMultiPartitionUdf() throws Exception {
		if (useMultiPartitionUdf()) {
			this.multiPartitionUdfClass = InvokeUtils.getAndValidUdf(this.partitionUdfClass, this.partitionUdf);
			if (this.multiPartitionUdfClass == null) {
				throw new IllegalArgumentException(String.format("udf class '%s' must be specified in udf '%s' ", this.partitionUdfClass, this.partitionUdf));
			}
			InvokeUtils.validMethod(multiPartitionUdfClass, this.partitionUdf);
			this.multiPartitionFieldIndex = findFieldIndex(this.partitionUdf);
		}
	}


	public boolean useMultiPartitionUdf() {
		return StringUtils.isNotBlank(this.partitionUdf) && StringUtils.isNotBlank(this.partitionUdfClass);
	}

	protected int[] findFieldIndex(String udfProperty) {
		String[] fieldNames = getFieldNames();
		int[] filedIndex;
		String[] inputColumns = udfProperty.split("\\(")[1].replaceAll("\\)", "").trim().split(",");
		filedIndex = new int[inputColumns.length];
		for (int i = 0; i < inputColumns.length; i++) {
			for (int j = 0; j < fieldNames.length; j++) {
				if (inputColumns[i].trim().equals(fieldNames[j])) {
					filedIndex[i] = j;
					LOG.info("input column:{}, field index:{}", inputColumns[i].trim(), j);
				}
			}
		}
		return filedIndex;
	}


	public Class<?>[] getFieldClass() {
		Class<?>[] className = new Class[fields.size()];
		for (int index = 0; index < className.length; index++) {
			className[index] = fields.get(index).getType().getClass();
		}
		return className;
	}

	public String[] getFieldNames() {
		String[] fieldNames = new String[fields.size()];
		for (int index = 0; index < fieldNames.length; index++) {
			fieldNames[index] = fields.get(index).getName();
		}
		return fieldNames;
	}

	protected TypeInformation<?>[] getTypeInformation(Class<?>[] fieldClass) {
		TypeInformation<?>[] typeInformation = new TypeInformation[fieldClass.length];
		for (int i = 0; i < fieldClass.length; i++) {
			typeInformation[i] = TypeInformation.of(SchemaUtil.classConvertSchemaName(fieldClass[i]));
		}
		return typeInformation;
	}
}
