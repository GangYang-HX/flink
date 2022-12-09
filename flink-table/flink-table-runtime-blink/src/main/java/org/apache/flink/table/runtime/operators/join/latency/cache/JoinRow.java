package org.apache.flink.table.runtime.operators.join.latency.cache;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.data.RowData;

/**
 * @author zhangyang
 * @Date:2019/10/12
 * @Time:1:53 PM
 */
public class JoinRow implements Serializable {

	public static final String LEFT_INPUT = "left";
	public static final String RIGHT_INPUT = "right";
	public static final String OUTPUT = "output";

    public static final int JOINED = 1;
    private RowData row;
    private String          condition;
    private String          tableName;
    private int             joined = 0;
    private long            timestamp;

    public JoinRow(RowData row, String condition, String tableName) {
        this.row = row;
        this.condition = condition;
        this.tableName = tableName;
        this.timestamp = System.currentTimeMillis();
    }

    public RowData getRow() {
        return row;
    }

    public void setRow(RowData row) {
        this.row = row;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getJoined() {
        return joined;
    }

    public void setJoined(int joined) {
        this.joined = joined;
    }

	@Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        JoinRow row = (JoinRow) obj;
        if (row.getJoined() != this.joined) {
            return false;
        }

        if (!StringUtils.equals(row.getCondition(), this.condition)) {
            return false;
        }

        if (!StringUtils.equals(row.getTableName(), this.tableName)) {
            return false;
        }

        if (!(row.getTimestamp() == this.timestamp)) {
            return false;
        }

        if (!row.getRow().equals(this.row)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("row:[").append(row.toString()).append("]").append(",timestamp:").append(timestamp).append(",joined:").append(joined).append(",condition:").append(condition).append(",tableName:").append(tableName);
        return sb.toString();
    }
}
