package org.apache.flink.table.runtime.operators.join.latency.cache;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.join.latency.compress.Compress;
import org.apache.flink.table.runtime.operators.join.latency.serialize.SerializeService;
import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;

import redis.clients.jedis.ScanResult;

/**
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:10:47 AM
 */
public class MultiRedisCache<T> extends JoinCache<T> {

	private List<JoinCache> caches;

	public MultiRedisCache(String redisAddressList, SerializeService<T> serializer, Compress compress, long extraExpireMs,
						   long bucketNum, MetricGroup metrics, Class class0) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		super(serializer, metrics);
		caches = new ArrayList<>();

		List<String> splitRedisList = getRedisCacheList(redisAddressList);
		for (String redisAddress : splitRedisList) {
			if (BucketRedisCache.class.equals(class0)) {
				Constructor<T> constructor =  class0.getConstructor(String.class, SerializeService.class, Compress.class, long.class, long.class, MetricGroup.class);
				caches.add((JoinCache) constructor.newInstance(redisAddress, serializer, compress, extraExpireMs, bucketNum, metrics));
			} else if (SimpleRedisCache.class.equals(class0)) {
				Constructor<T> constructor =  class0.getConstructor(String.class, SerializeService.class, Compress.class, long.class, MetricGroup.class);
				caches.add((JoinCache) constructor.newInstance(redisAddress, serializer, compress, extraExpireMs, metrics));
			} else {
				throw new RuntimeException("unsupported cache class");
			}
		}
	}

	private List<String> getRedisCacheList(String redisAddressList) {
		List<String> redisCacheList = new ArrayList<>();
		for (String redisAddress : redisAddressList.split(SymbolsConstant.SEMICOLON)) {
			if (!StringUtils.isBlank(redisAddress)) {
				redisCacheList.add(redisAddress);
			}
		}
		return redisCacheList;
	}

	@Override
	protected T doGet(String key) throws IOException {
		JoinCache cache = caches.get(getCacheIndex(key));
		T keyBody = (T) cache.doGet(key);
		return keyBody;
	}

	@Override
	protected void doPut(String key, T row, long expireMs) throws IOException {
		JoinCache cache = caches.get(getCacheIndex(key));
		cache.doPut(key, row, expireMs);
	}

	@Override
	public void doDelete(String key) {
		JoinCache cache = caches.get(getCacheIndex(key));
		cache.doDelete(key);
	}

	@Override
	public Boolean doExist(String key) {
		JoinCache cache = caches.get(getCacheIndex(key));
		return cache.doExist(key);
	}

	private int getCacheIndex(String key) {
		return Math.abs(key.hashCode()) % caches.size();
	}

	@Override
	protected T doGet(String key, String field) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void doPut(String key, String field, T row, long expireMs) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void doDelete(String key, String field) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected ScanResult<T> doScan(String key, String scanCursor, Integer count) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Boolean doExist(String key, String field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		for (JoinCache cache : caches) {
			cache.close();
		}
	}
}
