package com.bilibili.bsql.hdfs.table;

import com.bilibili.bsql.hdfs.function.BsqlHdfsSourceFunction;
import com.bilibili.bsql.hdfs.internal.FileSourceProperties;
import com.bilibili.bsql.hdfs.internal.selector.DateBasedSelector;
import com.bilibili.bsql.hdfs.internal.selector.FileSelector;
import com.bilibili.bsql.hdfs.tableinfo.HdfsSourceTableInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProviderWithParallel;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHdfsDynamicSource.java
 * @description This is the description of BsqlHdfsDynamicSource.java
 * @createTime 2020-11-04 19:06:00
 */
public class BsqlHdfsDynamicSource implements ScanTableSource {

    private final HdfsSourceTableInfo sourceTableInfo;
    protected final DataType outputDataType;
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public BsqlHdfsDynamicSource(HdfsSourceTableInfo sourceTableInfo, DataType outputDataType, DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.sourceTableInfo = sourceTableInfo;
        this.outputDataType = outputDataType;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return this.decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        FileSelector partitionSelector = new DateBasedSelector();
        DeserializationSchema<RowData> deserializationSchema = this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);
        int sourceParallel = sourceTableInfo.getParallelism();
        return SourceFunctionProviderWithParallel.of(
                new BsqlHdfsSourceFunction<>(buildProperties(), partitionSelector, deserializationSchema),
                false,
                sourceParallel
        );
    }

    private FileSourceProperties buildProperties() {
        FileSourceProperties fileSourceProperties = new FileSourceProperties();
        fileSourceProperties.setDelimiter(sourceTableInfo.getDelimiterKey());
        fileSourceProperties.setConfDir(sourceTableInfo.getConf());
        fileSourceProperties.setFetchSize(sourceTableInfo.getFetchSize());
        fileSourceProperties.setPath(sourceTableInfo.getPath());
        fileSourceProperties.setUserName(sourceTableInfo.getUser());
        TypeInformation<?>[] types = new TypeInformation[sourceTableInfo.getPhysicalFields().size()];
        for (int i = 0; i < sourceTableInfo.getFieldLogicalType().length; i++) {
            types[i] = TypeInformation.of(sourceTableInfo.getFieldLogicalType().getClass());
        }
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, sourceTableInfo.getFieldNames());
        fileSourceProperties.setSchema(rowTypeInfo);
        return fileSourceProperties;
    }

    @Override
    public DynamicTableSource copy() {
        return new BsqlHdfsDynamicSource(sourceTableInfo, outputDataType, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Hdfs-Source-" + sourceTableInfo.getName();
    }
}
