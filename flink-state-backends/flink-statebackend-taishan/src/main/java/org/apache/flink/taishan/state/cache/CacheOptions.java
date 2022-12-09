package org.apache.flink.taishan.state.cache;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author Dove
 * @Date 2022/10/11 3:26 下午
 */
public class CacheOptions {
	public static final ConfigOption<Integer> BATCH_WRITE_INTERVAL = ConfigOptions
		.key("state.backend.cache.write_interval")
		.intType()
		.defaultValue(300000)
		.withDescription("The interval that cache writes to persistent backend in batches. Time Unit: MilliSeconds");

	public static final ConfigOption<Integer> BATCH_WRITE_COUNT = ConfigOptions
		.key("state.backend.cache.write_count")
		.intType()
		.defaultValue(800)
		.withDescription("The count that cache writes to persistent backend in batches.");

	public static final ConfigOption<Integer> BATCH_WRITE_QUEUE_SIZE = ConfigOptions
		.key("state.backend.cache.write_queue_size")
		.intType()
		.defaultValue(10240)
		.withDescription("The queue size that holds the data that needs to be flushed to persistent backend");

	public static final ConfigOption<Integer> MAX_CACHE_SIZE = ConfigOptions
		.key("state.backend.cache.max_size")
		.intType()
		.defaultValue(100000)
		.withDescription("The max cache size.");

	public static final ConfigOption<Integer> EXPIRE_AFTER = ConfigOptions
		.key("state.backend.cache.expire_after")
		.intType()
		.defaultValue(15)
		.withDescription("Expire the entry after this duration. Time Unit: MINUTES");

//	public static final ConfigOption<Boolean> STATE_CACHE_ENABLED = ConfigOptions
//		.key("state.backend.cache.enabled")
//		.booleanType()
//		.defaultValue(true)
//		.withDescription("Whether state cache is enabled.");

	public static final ConfigOption<Boolean> CACHE_DYNAMIC_ENABLED = ConfigOptions
		.key("state.backend.cache.dynamic_enabled")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether dynamic cache is enabled.");

	public static final ConfigOption<Long> CACHE_MAX_GC_TIME = ConfigOptions
		.key("state.backend.cache.max_gc_time")
		.longType()
		.defaultValue(5 * 1000L)
		.withDescription("Max gc time to trigger cache clean. TIME UNIT: milliseconds");

	public static final ConfigOption<Double> CACHE_MAX_HEAP_USAGE = ConfigOptions
		.key("state.backend.cache.max_heap_usage")
		.doubleType()
		.defaultValue(0.9d)
		.withDescription("Max heap usage to trigger heap clean.");

	public static final ConfigOption<Boolean> BLOOM_FILTER_ENABLED = ConfigOptions
		.key("state.backend.cache.bloom_filter_enabled")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether bloom filter is enabled.");

	public static final ConfigOption<Integer> BLOOM_FILTER_NUMBER = ConfigOptions
		.key("state.backend.cache.bloom_filter_number")
		.intType()
		.defaultValue(0)
		.withDescription("Number of bloom filter per state. They are segmented");

	public static final ConfigOption<Double> BLOOM_FILTER_ALL_KEYS_RATIO = ConfigOptions
		.key("state.backend.cache.bloom_filter.offHeap.key-ratio")
		.doubleType()
		.defaultValue(0.4)
		.withDescription("If offheap is used to store all Keys, BLOOM_FILTER_NUMBER must be set to 0.");

	public static final ConfigOption<Long> BLOOM_FILTER_ALL_KEYS_MAX_SIZE = ConfigOptions
		.key("state.backend.cache.bloom_filter.offHeap.maxsize")
		.longType()
		.defaultValue(1024L * 1024 * 1024 * 1024) // 1024GB
		.withDescription("");

	public static final ConfigOption<String> CACHE_TYPE = ConfigOptions
		.key("state.backend.cache.type")
		.stringType()
		.defaultValue("offheap")
		.withDescription("Statebackend cache type(heap/offheap/none).");

	public static final ConfigOption<Double> CACHE_OFF_HEAP_BUFFER_RATIO = ConfigOptions
		.key("state.backend.cache.offheap.buffer-ratio")
		.doubleType()
		.defaultValue(0.5);

	public static final ConfigOption<Boolean> CACHE_OFF_HEAP_TTL_ENABLE = ConfigOptions
		.key("state.backend.cache.offheap.ttl.enable")
		.booleanType()
		.defaultValue(false);
}
