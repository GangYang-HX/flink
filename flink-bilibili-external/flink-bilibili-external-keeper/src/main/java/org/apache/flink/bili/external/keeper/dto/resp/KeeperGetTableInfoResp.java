package org.apache.flink.bili.external.keeper.dto.resp;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/** the GetTableInfo method return object. */
@NoArgsConstructor
@Data
public class KeeperGetTableInfoResp {

    private Database database;
    private String tableName;
    private String description;
    private Map<String, String> properties;
    private String tabId;
    private List<Column> columns;

    @NoArgsConstructor
    @Data
    public static class Database {
        private String dbType;
        private String dataServiceName;
        private String databaseName;
        private String description;
        private Map<String, String> properties;
        private String dsId;
        private String dbId;
    }

    @NoArgsConstructor
    @Data
    public static class Column {
        private String colName;
        private String colType;
        private String description;
        private String mappingColTypeEnum;
        private int colIndex;
        private boolean isPrimary;
        private boolean isPartition;
    }
}
