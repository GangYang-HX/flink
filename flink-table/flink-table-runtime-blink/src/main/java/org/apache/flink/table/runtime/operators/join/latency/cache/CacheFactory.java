package org.apache.flink.table.runtime.operators.join.latency.cache;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.compress.Compress;
import org.apache.flink.table.runtime.operators.join.latency.compress.CompressFactory;
import org.apache.flink.table.runtime.operators.join.latency.serialize.JoinRowSerializer;
import org.apache.flink.table.runtime.operators.join.latency.serialize.SerializeService;


/**
 * @author zhangyang
 * @Date:2019/12/13
 * @Time:11:42 AM
 */
public class CacheFactory {

	public static JoinCache<JoinRow> produce(CacheConfig cacheConfig, MetricGroup metricGroup, JoinRowSerializer serializer) throws Exception {
		JoinCache<JoinRow> cache;
		SerializeService serializeService = serializer;
		Compress compress = CompressFactory.produce(cacheConfig.getCompressType(), metricGroup);
		if (cacheConfig.getType() == JoinCacheEnums.BUCKET.getCode()) {
			cache = new BucketRedisCache<>(cacheConfig.getAddress(), serializeService, compress, 600_000,
				cacheConfig.getBucketNum(), metricGroup);
		} else if (cacheConfig.getType() == JoinCacheEnums.LETTUCE.getCode()) {
			cache = new LettuceRedisCache(cacheConfig.getAddress(), serializeService, compress, 600_000,
				cacheConfig.getBucketNum(), metricGroup);
		} else if (cacheConfig.getType() == JoinCacheEnums.HASH.getCode()) {
			cache = new HashRedisCache(cacheConfig.getAddress(), serializeService, compress, 600_000, metricGroup);
		} else if (cacheConfig.getType() == JoinCacheEnums.MULTI_BUCKET.getCode()) {
			cache = new MultiRedisCache(cacheConfig.getAddress(), serializeService, compress, 600_000,
				cacheConfig.getBucketNum(), metricGroup, BucketRedisCache.class);
		} else if (cacheConfig.getType() == JoinCacheEnums.MULTI_SIMPLE.getCode()) {
			cache = new MultiRedisCache(cacheConfig.getAddress(), serializeService, compress, 600_000,
				cacheConfig.getBucketNum(), metricGroup, SimpleRedisCache.class);
		} else {
			cache = new SimpleRedisCache<>(cacheConfig.getAddress(), serializeService, compress, 600_000, metricGroup);
		}
		return cache;
	}
}
