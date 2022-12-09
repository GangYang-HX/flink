package com.bilibili.bsql.hive.diversion.tableinfo;

import com.bilibili.bsql.hive.tableinfo.HiveRewriter;
import com.bilibili.bsql.hive.tableinfo.HiveSinkTableInfo;
import com.bilibili.bsql.hive.utils.HadoopPathUtils;
import com.bilibili.bsql.hive.utils.InvokeUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.bilibili.bsql.hive.tableinfo.HiveConfig.*;

/**
 * @author: zhuzhengjun
 * @date: 2022/2/28 4:26 下午
 */
@Data
public class MultiHiveSinkTableInfo extends HiveSinkTableInfo implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(MultiHiveSinkTableInfo.class);
	private Class<?> multiTableMetaUdfClass;
	private String tableMetaUdf;
	private String tableMetaUdfClass;
	private Class<?> customSplitPolicyClass;
	private String customSplitPolicyUdf;
	private String customSplitPolicyUdfClass;
	private String[] tableInfo;
	private String[] locations;
	private Class<?> tableTagUdfClassMode;
	private String tableTagUdf;
	private String tableTagClass;
	private int[] tableTagFiledIndex;
	private final String fieldDelim;
	private final String rowDelim;
	private final String partitionKey;
	private final String format;



	private transient Method getTableInfoMethod;
	private UserDefinedFunction tableMetaInstance;
	private Map<String, Double> tableMeta;
	private Map<String, ObjectIdentifier> objectIdentifierMap;
	private Map<String, String> tablePathMap;

	private transient Method getCustomSplitPolicyMethod;
	private UserDefinedFunction customSplitPolicyInstance;
	private Map<String, Double> customSplitPolicyMap;


	public MultiHiveSinkTableInfo(TableSinkFactory.Context context) {
		super(context);
		this.tableMetaUdf = props.get(BSQL_TABLE_META_UDF.key());
		this.tableMetaUdfClass = props.get(BSQL_TABLE_META_UDF_CLASS.key());
		this.multiTableMetaUdfClass = InvokeUtils.getAndValidUdf(tableMetaUdfClass, tableMetaUdf);
		this.customSplitPolicyUdf = props.get(BSQL_SPLIT_POLICY_UDF.key());
		this.customSplitPolicyUdfClass = props.get(BSQL_SPLIT_POLICY_UDF_CLASS.key());
		if (StringUtils.isNotBlank(this.customSplitPolicyUdf) && (StringUtils.isNotBlank(this.customSplitPolicyUdfClass))) {
			this.customSplitPolicyClass = InvokeUtils.getAndValidUdf(customSplitPolicyUdfClass, customSplitPolicyUdf);
			try {
				this.genCustomPolicy();
			} catch (Exception e) {
				LOG.error("Multi sink get custom split policy error! ", e);
			}

		}
		this.tableTagUdf = props.get(BSQL_TABLE_TAG_UDF.key());
		this.tableTagClass = props.get(BSQL_TABLE_TAG_UDF_CLASS.key());
		this.tableTagUdfClassMode = InvokeUtils.getAndValidUdf(tableTagClass, tableTagUdf);
		this.tableTagFiledIndex = findFieldIndex(this.tableTagUdf);
		try {
			genMeta();
			this.genLocations();
		} catch (Exception e) {
			LOG.error("Multi sink get table meta error! ", e);
		}
		String[] examTableName = tableInfo[0].split("\\.");
		try {
			HiveRewriter.rewrite(props, examTableName[0], examTableName[1]);
		} catch (Exception e) {
			LOG.error("Can not rewrite hive props.", e);
		}
		this.partitionKey = props.get(BSQL_HIVE_PARTITION_KEY.key());
		partitionKeyList = new ArrayList<>(Arrays.asList(partitionKey.split(";")));
		this.format = props.get(BSQL_HIVE_FORMAT.key());
		this.fieldDelim = props.get(BSQL_HIVE_FIELD_DELIM.key());
		this.rowDelim = props.get(BSQL_HIVE_ROW_DELIM.key());

		this.genObjectIdentifierMap();

	}

	private void genMeta() throws Exception {
		getTableInfoMethod = InvokeUtils.initMethod(multiTableMetaUdfClass);
		this.tableMetaInstance = InvokeUtils.initInstance(multiTableMetaUdfClass);
		try {
			this.tableMeta = (Map<String, Double>) getTableInfoMethod.invoke(tableMetaInstance, new Object[]{});
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new Exception("Multi table sink invoke table meta udf class failed!");
		}
		this.tableInfo = tableMeta.keySet().toArray(new String[0]);
	}

	private void genCustomPolicy() throws Exception {
		getCustomSplitPolicyMethod = InvokeUtils.initMethod(customSplitPolicyClass);
		this.customSplitPolicyInstance = InvokeUtils.initInstance(customSplitPolicyClass);
		try {
			this.customSplitPolicyMap = (Map<String, Double>) getCustomSplitPolicyMethod.invoke(customSplitPolicyInstance, new Object[]{});
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new Exception("get custom split policy error!");
		}
	}

	/**
	 * gen multi sink locations.
	 */
	private void genLocations() throws Exception {
		this.locations = new String[tableInfo.length];
		for (int i = 0; i < tableInfo.length; i++) {
//			this.locations[i] = "hdfs://uat-bigdata-ns2/department/ai/warehouse/tmp_fm_fym/";
			String[] table = tableInfo[i].split("\\.");
			HiveRewriter.HiveMetaDetail hiveMetaDetail = HiveRewriter.getMetaData(table[0], table[1]);
			this.locations[i] = HadoopPathUtils.verificationPath(hiveMetaDetail.getLocation());
		}
	}

	private void genObjectIdentifierMap() {
		Map<String, ObjectIdentifier> objectIdentifierMap = new HashMap<>();
		Map<String, String> tablePathMap = new HashMap<>();
		for (int i = 0; i < tableInfo.length; i++) {
			String[] table = tableInfo[i].split("\\.");
			objectIdentifierMap.put(tableInfo[i], ObjectIdentifier.of(this.locations[i], table[0], table[1]));
			tablePathMap.put(tableInfo[i], new Path(this.locations[i]).getPath());
		}
		this.objectIdentifierMap = objectIdentifierMap;
		this.tablePathMap = tablePathMap;

	}





}
