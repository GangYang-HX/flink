package com.bilibili.bsql.hive.tableinfo;

import com.bilibili.bsql.hive.utils.HadoopPathUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.table.factories.TableSinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import static com.bilibili.bsql.common.keys.TableInfoKeys.SABER_JOB_ID;
import static com.bilibili.bsql.hive.tableinfo.HiveConfig.*;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HiveSinkTableInfo.java
 * @description This is the description of HiveSinkTableInfo.java
 * @createTime 2020-10-28 16:20:00
 */
@Data
public class SingleHiveSinkTableTableInfo extends HiveSinkTableInfo implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(SingleHiveSinkTableTableInfo.class);
	private static final String CURR_TYPE = "hive";
    private final String tableName;
    private final String location;
	private final String fieldDelim;
	private final String rowDelim;
	private final String partitionKey;
	private final String format;

    public SingleHiveSinkTableTableInfo(TableSinkFactory.Context context)  {
		super(context);
		setType(CURR_TYPE);
        this.rawFields = context.getTable().getSchema().getTableColumns();

        this.tableName = props.get(BSQL_HIVE_TABLE_NAME.key());
        checkArgument(StringUtils.contains(tableName, "."), String.format("sink表:%s 必须为DB.TABLE_NAME", tableName));
        try {
            HiveRewriter.rewrite(props);
        } catch (Exception e) {
            LOG.error("Can not rewrite hive props.", e);
        }
        this.jobId = props.get(SABER_JOB_ID.key());
		this.location = HadoopPathUtils.verificationPath(props.get(BSQL_HIVE_LOCATION.key()));


        checkArgument(StringUtils.isNotEmpty(tableName), String.format("sink表:%s 没有设置tableName属性", tableName));

		LOG.info("catelog name :{} ,database name :{},object name:{}", context.getObjectIdentifier().getCatalogName(), context.getObjectIdentifier().getDatabaseName(), context.getObjectIdentifier().getObjectName());
		String[] dbTable = this.tableName.split("\\.");
		this.objectIdentifier = ObjectIdentifier.of(context.getObjectIdentifier().getCatalogName(), dbTable[0], dbTable[1]);
		this.fieldDelim = props.get(BSQL_HIVE_FIELD_DELIM.key());
		this.rowDelim = props.get(BSQL_HIVE_ROW_DELIM.key());
		partitionKey = props.get(BSQL_HIVE_PARTITION_KEY.key());
		partitionKeyList = new ArrayList<>(Arrays.asList(partitionKey.split(";")));
		format = props.get(BSQL_HIVE_FORMAT.key());
	}



}
