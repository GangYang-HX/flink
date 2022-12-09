package org.apache.flink.table.runtime.operators.join.latency.rockscache;

import java.io.Serializable;

import org.apache.flink.table.data.RowData;

public class CacheEntry implements Serializable {

	public CacheEntryType entryType;
	public RowData row;
	public long timestamp;

    public CacheEntry(RowData row, CacheEntryType entryType, long timestamp) {
        this.row = row;
        this.entryType = entryType;
        this.timestamp = timestamp;
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

	public CacheEntryType getEntryType() {
		return entryType;
	}

	public void setEntryType(CacheEntryType entryType) {
		this.entryType = entryType;
	}

	@Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        CacheEntry row = (CacheEntry) obj;
        if (row.getEntryType() != this.entryType) {
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
        sb.append("row:[")
			.append(row.toString())
			.append("]")
			.append(",timestamp:")
			.append(timestamp)
			.append(",join type:")
			.append(entryType);

        return sb.toString();
    }
}
