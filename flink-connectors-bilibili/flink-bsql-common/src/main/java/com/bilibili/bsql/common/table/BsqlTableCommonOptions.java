package com.bilibili.bsql.common.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** BsqlTableCommonOptions. */
public class BsqlTableCommonOptions {

    public static final String PARALLELISM_KEY = "parallelism";

    public static final String DELIMITER_KEY = "delimiterKey";
    public static final String SABER_JOB_ID_KEY = "saber-job-id";
    public static final String SABER_JOB_TAG_KEY = "saber-job-tag";

    public static final ConfigOption<String> SABER_JOB_ID =
            ConfigOptions.key(SABER_JOB_ID_KEY)
                    .stringType()
                    .defaultValue("undefined")
                    .withDescription("for saber to pass the job id parameter to runtime");

    public static final ConfigOption<String> SABER_JOB_TAG =
            ConfigOptions.key(SABER_JOB_TAG_KEY)
                    .stringType()
                    .defaultValue("")
                    .withDescription("for saber to pass the job tag parameter to runtime");

    public static final ConfigOption<Integer> BSQL_PARALLELISM =
            ConfigOptions.key(PARALLELISM_KEY)
                    .intType()
                    // default value = -1 in Flink 1.11
                    .defaultValue(-1)
                    .withDescription(
                            "the parallelism of the source, the default value is -1, means ");
    // delimiterKey Has become a format option, has to be sql transformed before parsing

    /**
     * from SinkTableInfo class BATCH_MAX_COUNT and BATCH_SIZE are the same, they are used to set
     * the batch size.
     */
    public static final String BATCH_MAX_TIMEOUT = "batchMaxTimeout";

    public static final String BATCH_SIZE = "batchSize";

    public static final ConfigOption<Integer> BSQL_BATCH_TIMEOUT =
            ConfigOptions.key(BATCH_MAX_TIMEOUT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("batch max count");

    public static final ConfigOption<Integer> BSQL_BATCH_SIZE =
            ConfigOptions.key(BATCH_SIZE).intType().noDefaultValue().withDescription("batch size");

    /**
     * from SideTableInfo class ASYNC_TIMEOUT and ASYNC_CAPACITY are global now, not sure whether
     * still needed PARTITIONED_JOIN_KEY not needed.
     */
    public static final String SIDE_MULTIKEY_DELIMITKEY = "delimitKey";

    public static final String CACHE_KEY = "cache";
    public static final String CACHE_SIZE_KEY = "cacheSize";
    public static final String CACHE_TTL_MS_KEY = "cacheTTLMs";

    public static final ConfigOption<String> BSQL_MULTIKEY_DELIMITKEY =
            ConfigOptions.key(SIDE_MULTIKEY_DELIMITKEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("side table multi key delimit key");

    public static final ConfigOption<String> BSQL_CACHE =
            ConfigOptions.key(CACHE_KEY)
                    .stringType()
                    .defaultValue("none")
                    .withDescription("cache type");

    public static final ConfigOption<Integer> BSQL_CACHE_SIZE =
            ConfigOptions.key(CACHE_SIZE_KEY)
                    .intType()
                    .defaultValue(10000)
                    .withDescription("cache size");

    public static final ConfigOption<Long> BSQL_CACHE_TTL =
            ConfigOptions.key(CACHE_TTL_MS_KEY)
                    .longType()
                    .defaultValue(60 * 1000L)
                    .withDescription("cache timeout");

    /**
     * Flink supports 'error' (default) and 'drop' enforcement behavior. By default, Flink will
     * check values and throw runtime exception when null values writing into NOT NULL columns.
     * Users can change the behavior to 'drop' to silently drop such records without throwing
     * exception.
     */
    public static final String NOT_NULL_ENFORCER_KEY = "table.exec.sink.not-null-enforcer";

    public static final ConfigOption<String> NOT_NULL_ENFORCER =
            ConfigOptions.key(NOT_NULL_ENFORCER_KEY)
                    .stringType()
                    .defaultValue("drop")
                    .withDescription(
                            "The NOT NULL column constraint on a table enforces that null values can't be inserted into the table.");
}
