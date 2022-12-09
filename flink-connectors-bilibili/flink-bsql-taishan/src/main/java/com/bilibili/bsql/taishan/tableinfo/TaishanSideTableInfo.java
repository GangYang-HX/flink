package com.bilibili.bsql.taishan.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.taishan.tableinfo.TaishanConfig.*;
import static com.bilibili.bsql.taishan.tableinfo.TaishanConfig.BSQL_TAISHAN_PASSWORD;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.util.Preconditions.checkArgument;

public class TaishanSideTableInfo extends SideTableInfo {
    // side table properties
    private final String cluster;
    private final String zone;
    private final String table;
    private final String password;
    private boolean multiKey = false;
    public Integer asyncTimeout;

    //runtime properties
    private final Integer keyIndex;
    private Integer fieldIndex = -1;
    private Integer valueIndex;
	private Integer keepAliveTime;
	private Integer keepAliveTimeout;
	private Integer idleTimeout;

    public TaishanSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        this.cluster = helper.getOptions().get(BSQL_TAISHAN_CLUSTER);
        this.zone = helper.getOptions().get(BSQL_TAISHAN_ZONE);
        this.table = helper.getOptions().get(BSQL_TAISHAN_TABLE);
        this.asyncTimeout = Integer.parseInt(helper.getOptions().get(BSQL_TAISHAN_TIMEOUT));
        this.password = helper.getOptions().get(BSQL_TAISHAN_PASSWORD);
        this.keyIndex = getPrimaryKeyIdx().get(0);
        this.type = "taishan";
		this.keepAliveTime = helper.getOptions().get(BSQL_TAISHAN_KEEP_ALIVE_TIME);
		this.keepAliveTimeout = helper.getOptions().get(BSQL_TAISHAN_KEEP_ALIVE_TIMEOUT);
		this.idleTimeout = helper.getOptions().get(BSQL_TAISHAN_IDLE_TIMEOUT);
        if (getPrimaryKeyIdx().size() == 2) {
            this.fieldIndex = getPrimaryKeyIdx().get(1);
        }
        for (int i = 0; i < getPhysicalFields().size(); i++) {
            if (!getPrimaryKeyIdx().contains(i)) {
                this.valueIndex = i;
                break;
            }
        }
        if (multiKeyDelimitor != null) {
            checkArgument(getPhysicalFields().get(valueIndex).getType().getTypeRoot() == ARRAY,
                    "'delimitKey' only support multiple key!");
            this.multiKey = true;
        }

        if (!"none".equals(getCacheType()) && multiKey) {
            throw new UnsupportedOperationException("taishan multi query not support the cache for now!!!!! ");
        }
    }

    public String getCluster() {
        return cluster;
    }

    public String getZone() {
        return zone;
    }

    public String getTable() {
        return table;
    }

    public String getPassword() {
        return password;
    }

    public Integer getKeyIndex() {
        return keyIndex;
    }

    public Integer getFieldIndex() {
        return fieldIndex;
    }

    public Integer getValueIndex() {
        return valueIndex;
    }

    public boolean isMultiKey() {
        return multiKey;
    }

    @Override
    public Integer getAsyncTimeout() {
        return this.asyncTimeout;
    }

	public Integer getKeepAliveTime() {
		return keepAliveTime;
	}

	public void setKeepAliveTime(Integer keepAliveTime) {
		this.keepAliveTime = keepAliveTime;
	}

	public Integer getKeepAliveTimeout() {
		return keepAliveTimeout;
	}

	public void setKeepAliveTimeout(Integer keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
	}

	public Integer getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Integer idleTimeout) {
		this.idleTimeout = idleTimeout;
	}
}
