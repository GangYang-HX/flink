
package com.bilibili.bsql.kafka.diversion.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Factory for creating configured instances of {@link DataGenTableSource} in a stream environment.
 */
@PublicEvolving
public class DataGenTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "bsql-datagen";
    public static final Long ROWS_PER_SECOND_DEFAULT_VALUE = 10000L;
    public static final Long NUMBER_OF_ROWS_DEFAULT_VALUE = 10000L;

    public static final ConfigOption<Long> ROWS_PER_SECOND =
            key("rows-per-second")
                    .longType()
                    .defaultValue(ROWS_PER_SECOND_DEFAULT_VALUE)
                    .withDescription("Rows per second to control the emit rate.");

    public static final ConfigOption<Long> NUMBER_OF_ROWS =
            key("number_of_rows")
                    .longType()
                    .defaultValue(NUMBER_OF_ROWS_DEFAULT_VALUE)
                    .withDescription("Rows per second to control the emit rate.");

    public static final String FIELDS = "fields";
    public static final String KIND = "kind";
    public static final String START = "start";
    public static final String END = "end";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String LENGTH = "length";

    public static final String SEQUENCE = "sequence";
    public static final String RANDOM = "random";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROWS_PER_SECOND);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);

        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        DataGenerator[] fieldGenerators = new DataGenerator[tableSchema.getFieldCount()];
        for (int i = 0; i < fieldGenerators.length; i++) {
            fieldGenerators[i] =
                    createDataGenerator(
                            tableSchema.getFieldName(i).get(),
                            tableSchema.getFieldDataType(i).get(),
                            options);
        }

        return new DataGenTableSource(
                fieldGenerators,
                tableSchema,
                options.get(ROWS_PER_SECOND),
                options.get(NUMBER_OF_ROWS));
    }

    private DataGenerator createDataGenerator(String name, DataType type, ReadableConfig options) {
        String genType =
                options.get(
                        key(FIELDS + "." + name + "." + KIND).stringType().defaultValue(RANDOM));
        switch (genType) {
            case RANDOM:
                return createRandomGenerator(name, type, options);
            case SEQUENCE:
                return createSequenceGenerator(name, type, options);
            default:
                throw new ValidationException("Unsupported generator type: " + genType);
        }
    }

    private DataGenerator createRandomGenerator(
            String name, DataType type, ReadableConfig options) {
        ConfigOption<Integer> lenKey =
                key(FIELDS + "." + name + "." + LENGTH).intType().defaultValue(100);
        ConfigOptions.OptionBuilder minKey = key(FIELDS + "." + name + "." + MIN);
        ConfigOptions.OptionBuilder maxKey = key(FIELDS + "." + name + "." + MAX);
        switch (type.getLogicalType().getTypeRoot()) {
            case BOOLEAN:
                return RandomGenerator.booleanGenerator();
            case CHAR:
            case VARCHAR:
                int length = options.get(lenKey);
                return getRandomStringGenerator(length);
            case TINYINT:
                return RandomGenerator.byteGenerator(
                        options.get(minKey.intType().defaultValue((int) Byte.MIN_VALUE))
                                .byteValue(),
                        options.get(maxKey.intType().defaultValue((int) Byte.MAX_VALUE))
                                .byteValue());
            case SMALLINT:
                return RandomGenerator.shortGenerator(
                        options.get(minKey.intType().defaultValue((int) Short.MIN_VALUE))
                                .shortValue(),
                        options.get(maxKey.intType().defaultValue((int) Short.MAX_VALUE))
                                .shortValue());
            case INTEGER:
                return RandomGenerator.intGenerator(
                        options.get(minKey.intType().defaultValue(Integer.MIN_VALUE)),
                        options.get(maxKey.intType().defaultValue(Integer.MAX_VALUE)));
            case BIGINT:
                return RandomGenerator.longGenerator(
                        options.get(minKey.longType().defaultValue(Long.MIN_VALUE)),
                        options.get(maxKey.longType().defaultValue(Long.MAX_VALUE)));
            case FLOAT:
                return RandomGenerator.floatGenerator(
                        options.get(minKey.floatType().defaultValue(Float.MIN_VALUE)),
                        options.get(maxKey.floatType().defaultValue(Float.MAX_VALUE)));
            case DOUBLE:
                return RandomGenerator.doubleGenerator(
                        options.get(minKey.doubleType().defaultValue(Double.MIN_VALUE)),
                        options.get(maxKey.doubleType().defaultValue(Double.MAX_VALUE)));
            default:
                throw new ValidationException("Unsupported type: " + type);
        }
    }

    private static RandomGenerator<StringData> getRandomStringGenerator(int length) {
        return new RandomGenerator<StringData>() {
            @Override
            public StringData next() {
                return StringData.fromString(random.nextHexString(length));
            }
        };
    }

    private DataGenerator createSequenceGenerator(
            String name, DataType type, ReadableConfig options) {
        String startKeyStr = FIELDS + "." + name + "." + START;
        String endKeyStr = FIELDS + "." + name + "." + END;
        ConfigOptions.OptionBuilder startKey = key(startKeyStr);
        ConfigOptions.OptionBuilder endKey = key(endKeyStr);

        options.getOptional(startKey.stringType().noDefaultValue())
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Could not find required property '"
                                                + startKeyStr
                                                + "' for sequence generator."));
        options.getOptional(endKey.stringType().noDefaultValue())
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Could not find required property '"
                                                + endKeyStr
                                                + "' for sequence generator."));

        switch (type.getLogicalType().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return getSequenceStringGenerator(
                        options.get(startKey.longType().noDefaultValue()),
                        options.get(endKey.longType().noDefaultValue()));
            case TINYINT:
                return SequenceGenerator.byteGenerator(
                        options.get(startKey.intType().noDefaultValue()).byteValue(),
                        options.get(endKey.intType().noDefaultValue()).byteValue());
            case SMALLINT:
                return SequenceGenerator.shortGenerator(
                        options.get(startKey.intType().noDefaultValue()).shortValue(),
                        options.get(endKey.intType().noDefaultValue()).shortValue());
            case INTEGER:
                return SequenceGenerator.intGenerator(
                        options.get(startKey.intType().noDefaultValue()),
                        options.get(endKey.intType().noDefaultValue()));
            case BIGINT:
                return SequenceGenerator.longGenerator(
                        options.get(startKey.longType().noDefaultValue()),
                        options.get(endKey.longType().noDefaultValue()));
            case FLOAT:
                return SequenceGenerator.floatGenerator(
                        options.get(startKey.intType().noDefaultValue()).shortValue(),
                        options.get(endKey.intType().noDefaultValue()).shortValue());
            case DOUBLE:
                return SequenceGenerator.doubleGenerator(
                        options.get(startKey.intType().noDefaultValue()),
                        options.get(endKey.intType().noDefaultValue()));
            default:
                throw new ValidationException("Unsupported type: " + type);
        }
    }

    private static SequenceGenerator<StringData> getSequenceStringGenerator(long start, long end) {
        return new SequenceGenerator<StringData>(start, end) {
            @Override
            public StringData next() {
                return StringData.fromString(valuesToEmit.poll().toString());
            }
        };
    }

    /**
     * A {@link StreamTableSource} that emits each number from a given interval exactly once,
     * possibly in parallel. See {@link StatefulSequenceSource}.
     */
    static class DataGenTableSource implements ScanTableSource {

        private final DataGenerator[] fieldGenerators;
        private final TableSchema schema;
        private final long rowsPerSecond;
        private final long numberOfRows;

        private DataGenTableSource(
                DataGenerator[] fieldGenerators,
                TableSchema schema,
                long rowsPerSecond,
                long numberOfRows) {
            this.fieldGenerators = fieldGenerators;
            this.schema = schema;
            this.rowsPerSecond = rowsPerSecond;
            this.numberOfRows = numberOfRows;
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
            return SourceFunctionProvider.of(createSource(), false);
        }

        @VisibleForTesting
        DataGeneratorSource<RowData> createSource() {
            return new DataGeneratorSource<>(
                    new RowGenerator(fieldGenerators, schema.getFieldNames()),
                    rowsPerSecond,
                    numberOfRows);
        }

        @Override
        public DynamicTableSource copy() {
            return new DataGenTableSource(fieldGenerators, schema, rowsPerSecond, numberOfRows);
        }

        @Override
        public String asSummaryString() {
            return "DataGenTableSource";
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }
    }

    private static class RowGenerator implements DataGenerator<RowData> {

        private static final long serialVersionUID = 1L;

        private final DataGenerator[] fieldGenerators;
        private final String[] fieldNames;

        private RowGenerator(DataGenerator[] fieldGenerators, String[] fieldNames) {
            this.fieldGenerators = fieldGenerators;
            this.fieldNames = fieldNames;
        }

        @Override
        public void open(
                String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
                throws Exception {
            for (int i = 0; i < fieldGenerators.length; i++) {
                fieldGenerators[i].open(fieldNames[i], context, runtimeContext);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            for (DataGenerator generator : fieldGenerators) {
                generator.snapshotState(context);
            }
        }

        @Override
        public boolean hasNext() {
            for (DataGenerator generator : fieldGenerators) {
                if (!generator.hasNext()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RowData next() {
            GenericRowData row = new GenericRowData(fieldNames.length);
            for (int i = 0; i < fieldGenerators.length; i++) {
                row.setField(i, fieldGenerators[i].next());
            }
            return row;
        }
    }
}
