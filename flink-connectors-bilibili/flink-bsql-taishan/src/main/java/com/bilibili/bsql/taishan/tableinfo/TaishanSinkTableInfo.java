package com.bilibili.bsql.taishan.tableinfo;

import com.bilibili.bsql.common.SinkTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.List;

import static com.bilibili.bsql.taishan.tableinfo.TaishanConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;


/**
 * @author zhuzhengjun
 * @date 2020/10/29 11:16 上午
 */

@Data
public class TaishanSinkTableInfo extends SinkTableInfo implements Serializable {
    private static final String CUR_TYPE = "taishan";

    private String cluster;
    private String zone;
    private String table;
    private String password;
    private int ttl;

	private Integer keyIndex;
	private Integer fieldIndex = -1;
	private Integer valueIndex;
	private Integer retryCount;
	private Integer duration;
	private Integer keepAliveTime;
	private Integer keepAliveTimeout;
	private Integer idleTimeout;
	private Integer retryInterval;

    public TaishanSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        this.cluster = helper.getOptions().get(BSQL_TAISHAN_CLUSTER);
        this.zone = helper.getOptions().get(BSQL_TAISHAN_ZONE);
        this.table = helper.getOptions().get(BSQL_TAISHAN_TABLE);
        this.password = helper.getOptions().get(BSQL_TAISHAN_PASSWORD);
        this.ttl = helper.getOptions().get(BSQL_TAISHAN_TTL);
		this.retryCount = helper.getOptions().get(BSQL_TAISHAN_RETRY_COUNT);
		this.duration = helper.getOptions().get(BSQL_TAISHAN_DURATION);
		this.keepAliveTime = helper.getOptions().get(BSQL_TAISHAN_KEEP_ALIVE_TIME);
		this.keepAliveTimeout = helper.getOptions().get(BSQL_TAISHAN_KEEP_ALIVE_TIMEOUT);
		this.idleTimeout = helper.getOptions().get(BSQL_TAISHAN_IDLE_TIMEOUT);
		this.retryInterval = helper.getOptions().get(BSQL_TAISHAN_RETRY_INTERVAL);

        checkArgument(StringUtils.isNotEmpty(cluster), String.format("sink表:%s 没有设置cluster属性", getName()));
        checkArgument(StringUtils.isNotEmpty(zone), String.format("sink表:%s 没有设置zone属性", getName()));
        checkArgument(StringUtils.isNotEmpty(table), String.format("sink表:%s 没有设置table属性", getName()));
        checkArgument(StringUtils.isNotEmpty(password), String.format("sink表:%s 没有设置password属性", getName()));
		checkArgument(getPrimaryKeyIdx().size() == 1 || getPrimaryKeyIdx().size() == 2,
			"taishan only support for one key");
		this.keyIndex = getPrimaryKeyIdx().get(0);

		if (getPrimaryKeyIdx().size() == 2) {
			this.fieldIndex = getPrimaryKeyIdx().get(1);
		}
		for (int i = 0; i < getPhysicalFields().size(); i++){
			if (!getPrimaryKeyIdx().contains(i)) {
				this.valueIndex = i;
				break;
			}
		}

    }

    @Override
    public String getType() {
        return super.getType();
    }

}
