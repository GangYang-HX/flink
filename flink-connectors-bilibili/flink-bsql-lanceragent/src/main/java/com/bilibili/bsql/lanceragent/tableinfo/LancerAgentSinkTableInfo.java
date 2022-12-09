package com.bilibili.bsql.lanceragent.tableinfo;

import com.bilibili.bsql.common.SinkTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.lanceragent.tableinfo.LancerAgentConfig.BSQL_LANCER_AGENT_DELIMITER;
import static com.bilibili.bsql.lanceragent.tableinfo.LancerAgentConfig.BSQL_LANCER_AGENT_LOG_ID;
import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className LancerAgentSinkTableInfo.java
 * @description This is the description of LancerAgentSinkTableInfo.java
 * @createTime 2020-11-10 18:24:00
 */
@Data
public class LancerAgentSinkTableInfo extends SinkTableInfo {
    private static final String CURR_TYPE = "lanceragent";
    private String logId;
    private String delimiter = "|";

    public LancerAgentSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        this.logId = helper.getOptions().get(BSQL_LANCER_AGENT_LOG_ID);
        this.delimiter = unicodeStringDecode(helper.getOptions().get(BSQL_LANCER_AGENT_DELIMITER));
        setType(CURR_TYPE);
        checkArgument(StringUtils.isNotEmpty(this.logId), String.format("sink表:%s 没有设置logId属性", getLogId()));
    }
}
