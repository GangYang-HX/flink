/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common;

import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.bilibili.bsql.common.keys.TableInfoKeys.*;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * @author zhouxiaogang
 * @version $Id: TableInfo.java, v 0.1 2020-10-13 11:25
 * zhouxiaogang Exp $$
 * <p>
 * to be compatible with saber TableInfo, create a similar struct
 */
@Data
@ToString
public abstract class TableInfo implements Serializable {
	private Logger LOG = LoggerFactory.getLogger(TableInfo.class);
    // table name
    public String name;
    // table type, corresponding to connector
    public String type = "default";
    public Integer parallelism;
    public String jobId;
    public String[] jobTags;

    public List<String> primaryKeys;
    public List<Integer> primaryKeyIdx;
    /**
     * this variable include computed part
     */
    public List<FieldInfo> fields;
    /**
     * this variable exclude computed part
     */
    public List<FieldInfo> physicalFields;

	/**
	 * this may contains morn than one config from common such as job id etc.
	 */
	public Map<String, String> customConfig;

    /**
     * because TableColumn is not serializable, so we transform it to the FieldInfo
     * the rawFields is not accessible in the runtime
     */
    public transient List<TableColumn> rawFields;
    // isSideTable is no longer needed


    public TableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		initCustomConfig(context);
        this.name = context.getObjectIdentifier().getObjectName();
        this.type = helper.getOptions().get(CONNECTOR);
        this.parallelism = helper.getOptions().get(BSQL_PARALLELISM);
		this.jobId = this.customConfig == null ? helper.getOptions().get(SABER_JOB_ID) :
			this.customConfig.getOrDefault(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID.key(),
				helper.getOptions().get(SABER_JOB_ID));
        this.jobTags = StringUtils.splitPreserveAllTokens(context.getConfiguration().get(SABER_JOB_TAG), ",");

        Optional<UniqueConstraint> primaryConstraint = context.getCatalogTable().getSchema().getPrimaryKey();
        if (primaryConstraint.isPresent()) {
            this.primaryKeys = primaryConstraint.get().getColumns();
        } else {
            this.primaryKeys = new ArrayList<>();
        }

        this.rawFields = context.getCatalogTable().
                getSchema().
                getTableColumns();
        this.fields = rawFields
                .stream().map(TableInfo::fromColumn).collect(Collectors.toList());
        this.physicalFields = this.fields
                .stream().filter(column -> !column.isGenerated()).collect(Collectors.toList());
        this.primaryKeyIdx = this.primaryKeys.stream().map(this::fieldIndex).collect(Collectors.toList());
    }
    public int fieldIndex(String fieldName) {
        for (int i = 0; i < physicalFields.size(); i++) {
            if (fieldName.equals(physicalFields.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    public static FieldInfo fromColumn(TableColumn tableColumn) {
        return new FieldInfo(
                tableColumn.getName(),
                tableColumn.getType().getLogicalType(),
                (tableColumn.isGenerated() && tableColumn.getExpr().isPresent()) ? tableColumn.getExpr().get() : null
        );
    }

	public DataType getPhysicalRowDataType() {
		final DataTypes.Field[] fields = rawFields.stream()
			.filter(TableColumn::isPhysical)
			.map(column -> FIELD(column.getName(), column.getType()))
			.toArray(DataTypes.Field[]::new);
		return ROW(fields);
	}

	public DataType getNonGeneratedRowDataType() {
		final DataTypes.Field[] fields = rawFields.stream()
			.filter(col -> col.isMetadata() || col.isPhysical())
			.map(column -> FIELD(column.getName(), column.getType()))
			.toArray(DataTypes.Field[]::new);
		return ROW(fields);
	}

	public int[] createPhysicalProjectIndex() {
		List<Integer> physicalIndices = new ArrayList<>();
		List<TableColumn> noGeneratedIndex = rawFields.stream()
			.filter(col -> !col.isGenerated())
			.collect(Collectors.toList());
		for (int i = 0; i < noGeneratedIndex.size(); i++) {
			TableColumn column = noGeneratedIndex.get(i);
			if (column.isPhysical()) {
				physicalIndices.add(i);
			}
		}
		return physicalIndices.stream().mapToInt(x -> x).toArray();
	}


    public String[] getFieldNames() {
        String[] fieldNames = new String[fields.size()];
        for (int index = 0; index < fieldNames.length; index++) {
            fieldNames[index] = fields.get(index).getName();
        }
        return fieldNames;
    }

    public LogicalType[] getFieldLogicalType() {
        LogicalType[] logicalTye = new LogicalType[fields.size()];
        for (int index = 0; index < logicalTye.length; index++) {
            logicalTye[index] = fields.get(index).getType();
        }
        return logicalTye;
    }

    @Data
    public static class FieldInfo implements Serializable {
        public String name;
        // 字段类型
        public LogicalType type;
        public String expr;

        public FieldInfo(String name, LogicalType type, String expr) {
            this.name = name;
            this.type = type;
            this.expr = expr;
        }

        public boolean isGenerated() {
            return this.expr != null;
        }
    }

	public void initCustomConfig(DynamicTableFactory.Context context) {
		this.customConfig = context.getConfiguration().get(PipelineOptions.GLOBAL_JOB_PARAMETERS);
		LOG.info("pipeline global job custom config : {}", JSON.toString(customConfig));
	}

}
