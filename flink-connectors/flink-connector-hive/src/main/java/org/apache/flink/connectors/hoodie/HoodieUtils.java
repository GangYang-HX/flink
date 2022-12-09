package org.apache.flink.connectors.hoodie;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** utils for hoodie. */
public class HoodieUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoodieUtils.class);

    public static boolean isHoodieTable(String format) {
        return isRoTable(format) || isRtTable(format);
    }

    public static String getTableType(Table table) {
        String format = table.getSd().getInputFormat();
        if (isRtTable(format)) {
            return HoodieTableType.MERGE_ON_READ.name();
        }

        if (isRoTable(format)) {
            return HoodieTableType.COPY_ON_WRITE.name();
        }

        throw new UnsupportedOperationException("illegal hoodie table");
    }

    private static boolean isRtTable(String format) {
        try {
            Class.forName(MergeOnReadInputFormat.class.getName());
            Class.forName(HoodieParquetRealtimeInputFormat.class.getName());
            if (format.equalsIgnoreCase(HoodieParquetRealtimeInputFormat.class.getName())) {
                return true;
            }
        } catch (Throwable e) {
            // ignore
        }
        return false;
    }

    private static boolean isRoTable(String format) {
        try {
            Class.forName(CopyOnWriteInputFormat.class.getName());
            Class.forName(HoodieParquetInputFormat.class.getName());
            if (format.equalsIgnoreCase(HoodieParquetInputFormat.class.getName())) {
                return true;
            }
        } catch (Throwable e) {
            // ignore
        }
        return false;
    }
}
