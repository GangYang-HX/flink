package com.bilibili.bsql.redis.sink.jedisbin.clusterpipeline;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisClusterInfoCache;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

/**
 * @author zhangyang
 * @Date:2020/3/16
 * @Time:1:28 PM
 */
public class ClusterPipeline extends PipelineBase implements Closeable {

    private static final Logger             LOGGER       = LoggerFactory.getLogger(ClusterPipeline.class);

    private static final Field              FIELD_CONNECTION_HANDLER;
    private static final Field              FIELD_CACHE;
    static {
        FIELD_CONNECTION_HANDLER = getField(BinaryJedisCluster.class, "connectionHandler");
        FIELD_CACHE = getField(JedisClusterConnectionHandler.class, "cache");
    }

    private JedisSlotBasedConnectionHandler connectionHandler;
    private JedisClusterInfoCache clusterInfoCache;
    private Queue<Client>                   clients      = new LinkedList<>();
    private Map<JedisPool, Jedis>           jedisMap     = new HashMap<>();
    private boolean                         hasDataInBuf = false;

    public ClusterPipeline(JedisCluster jedisCluster) {
        connectionHandler = getValue(jedisCluster, FIELD_CONNECTION_HANDLER);
        clusterInfoCache = getValue(connectionHandler, FIELD_CACHE);
    }

    /**
     * 同步读取所有数据. 与syncAndReturnAll()相比，sync()只是没有对数据做反序列化
     */
    public void sync() {
        try {
            for (Client client : clients) {
                generateResponse(client.getOne()).get();
            }
        } catch (JedisRedirectionException jre) {
            if (jre instanceof JedisMovedDataException) {
                LOGGER.info("renew slot cache");
                connectionHandler.renewSlotCache();
            }
            throw jre;
        } finally {
            for (Jedis jedis : jedisMap.values()) {
                flushCachedData(jedis);
            }
            hasDataInBuf = false;
            close();
        }
    }

    @Override
    public void close() {
        clean();
        clients.clear();
        for (Jedis jedis : jedisMap.values()) {
            if (hasDataInBuf) {
                flushCachedData(jedis);
            }
            jedis.close();
        }
        jedisMap.clear();
        hasDataInBuf = false;
    }

    private void flushCachedData(Jedis jedis) {
        try {
            jedis.getClient().getMany(0);
        } catch (RuntimeException ex) {
            // 其中一个client出问题，后面出问题的几率较大
        }
    }

    @Override
    protected Client getClient(String key) {
        byte[] bKey = SafeEncoder.encode(key);
        return getClient(bKey);
    }

    @Override
    protected Client getClient(byte[] key) {
        Jedis jedis = getJedis(JedisClusterCRC16.getSlot(key));
        Client client = jedis.getClient();
        clients.add(client);
        return client;
    }

    private Jedis getJedis(int slot) {
        JedisPool pool = clusterInfoCache.getSlotPool(slot);
        Jedis jedis = jedisMap.get(pool);
        if (null == jedis) {
            jedis = pool.getResource();
            jedisMap.put(pool, jedis);
        }

        hasDataInBuf = true;
        return jedis;
    }

    private static Field getField(Class<?> cls, String fieldName) {
        try {
            Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);

            return field;
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException("cannot find or access field '" + fieldName + "' from " + cls.getName(), e);
        }
    }

    private static <T> T getValue(Object obj, Field field) {
        try {
            return (T) field.get(obj);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            LOGGER.error("get value fail", e);

            throw new RuntimeException(e);
        }
    }
}
