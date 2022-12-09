package org.apache.flink.table.api;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className LineAgeInfo.java
 * @description This is the description of LineAgeInfo.java
 * @createTime 2020-11-30 16:00:00
 */
public class LineAgeInfo {
    private ObjectIdentifier objectIdentifier;
    private CatalogTable catalogTable;
    private TableType tableType;
    private String content;

    public enum TableType {
        /**
         * table type
         */
        SOURCE(1, "source"),
        SIDE(2, "side"),
        SINK(3, "sink");
        private final int index;
        private final String name;

        TableType(int index, String name) {
            this.index = index;
            this.name = name;
        }

        public int getIndex() {
            return index;
        }

        public String getName() {
            return name;
        }
    }

    public ObjectIdentifier getObjectIdentifier() {
        return objectIdentifier;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    public TableType getTableType() {
        return tableType;
    }

    public String getContent() {
        return content;
    }


    public static final class LineAgeInfoBuilder {
        private ObjectIdentifier objectIdentifier;
        private CatalogTable catalogTable;
        private TableType tableType;
        private String content;

        private LineAgeInfoBuilder() {
        }

        public static LineAgeInfoBuilder builder() {
            return new LineAgeInfoBuilder();
        }

        public LineAgeInfoBuilder withObjectIdentifier(ObjectIdentifier objectIdentifier) {
            this.objectIdentifier = objectIdentifier;
            return this;
        }

        public LineAgeInfoBuilder withCatalogTable(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
            return this;
        }

        public LineAgeInfoBuilder withTableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public LineAgeInfoBuilder withContent(String content) {
            this.content = content;
            return this;
        }

        public LineAgeInfo build() {
            LineAgeInfo lineAgeInfo = new LineAgeInfo();
            lineAgeInfo.tableType = this.tableType;
            lineAgeInfo.objectIdentifier = this.objectIdentifier;
            lineAgeInfo.catalogTable = this.catalogTable;
            lineAgeInfo.content = this.content;
            return lineAgeInfo;
        }
    }

    @Override
    public int hashCode(){
    	return objectIdentifier.getObjectName().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		LineAgeInfo lineAgeInfo = (LineAgeInfo) obj;
		return lineAgeInfo.getObjectIdentifier().getObjectName().equals(this.objectIdentifier.getObjectName());
	}
}
