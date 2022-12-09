package com.bilibili.bsql.hive.filetype.parquet.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.types.Row;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/**
 * Created by haozhugogo on 2020/5/5.
 */
public class BiliParquetAvroWriters {

    /**
     * Creates a ParquetWriterFactory for the given type. The Parquet writers will use Avro to reflectively create a
     * schema for the type and use that schema to write the columnar data.
     *
     * @param schemaString         The schema of the type to write.
     * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
     */
    public static ParquetWriterFactory<Row> forRow(String schemaString, CompressionCodecName compressionCodecName) {
        final ParquetBuilder<Row> builder = (out) -> createAvroParquetWriter(schemaString, ReflectData.get(), out,
                compressionCodecName);
        return new ParquetWriterFactory<>(builder);
    }

    private static ParquetWriter<Row> createAvroParquetWriter(String schemaString, GenericData dataModel,
                                                              OutputFile out,
                                                              CompressionCodecName compressionCodecName) throws IOException {

        final Schema schema = new Schema.Parser().parse(schemaString);

        return new BiliParquetWriterBuilder(out).withSchema(schema).withDataModel(dataModel).withCompressionCodec(compressionCodecName).build();
    }

    // ------------------------------------------------------------------------

    /**
     * Class is not meant to be instantiated.
     */
    private BiliParquetAvroWriters() {
    }
}
