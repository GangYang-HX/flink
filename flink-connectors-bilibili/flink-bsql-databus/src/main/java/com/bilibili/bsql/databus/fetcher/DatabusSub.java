package com.bilibili.bsql.databus.fetcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bilibili.bsql.databus.tableinfo.BsqlDataBusConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pleiades.component.databus.DataBuses;
import pleiades.component.databus.model.DatabusProperties;
import pleiades.component.metrics.BiliSimpleCollector;
import pleiades.component.model.DatabusClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DatabusSub.java
 * @description This is the description of DatabusSub.java
 * @createTime 2020-10-22 18:30:00
 */
@Slf4j
@Data
public class DatabusSub {
    private final Logger logger = LoggerFactory.getLogger(DatabusSub.class);
    private volatile List<InetSocketAddress> addresses;
    private final Random random = new Random();
    private final String password;
    private String host;
    private Jedis jedis;
    private final Lock lock = new ReentrantLock();
    private final Map<Integer, Long> lastCommitOffset = new ConcurrentHashMap<>();
    private String topic;
    private String groupId;
    private String appKey;
    private String appSecret;
    private final long intervalMillis = 5 * 1000;
    private final int maxAttempts = 18;
    private AtomicInteger retryCounter = new AtomicInteger(maxAttempts);

    public DatabusSub(Properties properties) throws Exception {
        this.topic = properties.getProperty(BsqlDataBusConfig.TOPIC_KEY);
        this.groupId = properties.getProperty(BsqlDataBusConfig.GROUPID_KEY);
        this.appKey = properties.getProperty(BsqlDataBusConfig.APPKEY_KEY);
        this.appSecret = properties.getProperty(BsqlDataBusConfig.APPSECRET_KEY);
        String address = properties.getProperty(BsqlDataBusConfig.ADDRESS_KEY);
        List<String> addressList = Arrays.asList(address.replace(" ", "").split(","));
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(groupId);
        Preconditions.checkNotNull(appKey);
        Preconditions.checkNotNull(appSecret);
        Preconditions.checkNotNull(address);
        this.password = getPwdSub(topic, groupId, appKey, appSecret);
        this.addresses = addressList.stream().map(add -> {
            String hostName = add.substring(0, add.indexOf(":"));
            String port = add.substring(add.indexOf(":") + 1);
            return new InetSocketAddress(hostName, Integer.parseInt(port));
        }).collect(Collectors.toList());
        switchAddress();
        reConnect();
        //initialJedis(10);//如果取不到连接则重试10次
    }

    private String getPwdSub(String topic, String groupId, String appKey, String appSecret) {
        return appKey + ":" + appSecret + "@" + groupId + "/topic=" + topic + "&role=" + "sub";
    }

    private String getPwdSub(DatabusProperties properties) {
        return properties.getAppkey() + ":" + properties.getSecret() + "@"
                + properties.getSub().getGroup() + "/topic=" + properties.getSub().getTopic()
                + "&role=" + "sub";
    }

    public boolean backOffFinish() {
        return retryCounter.get() < 0;
    }

    public boolean backOffNoStart() {
        return retryCounter.get() == maxAttempts;
    }

    private void resetBackOff() {
        this.retryCounter = new AtomicInteger(maxAttempts);
        logger.info("{} Reset back off!", Thread.currentThread().getName());
    }

    /**
     * jedis连接过快会导致databus rebalance未完成从而报错，所以应等待rebalance完成再让jedis连接
     *
     * @throws InterruptedException
     */
    private synchronized void reConnect() throws Exception {
        int times = 1;
        while (!backOffFinish()) {
            try {
                if (jedis == null || !validateObject()) {
                    this.retryCounter.decrementAndGet();
                    close();
                    jedis = getNewRedisClient();
                    logger.info("{} Get redis connect success.", Thread.currentThread().getName());
                    resetBackOff();
                }
                break;
            } catch (JedisException e) {
                String msg = DataBuses.messageFromEx(e);
				if("useless consumer".equals(e.getMessage())){
					close();
					throw new RuntimeException(e);
				}
				Thread.sleep(intervalMillis);
                logger.error("Can not get jedis = {} connect,retry it " + times++ + " times, case = {}， thread = {}", this.addresses, msg, Thread.currentThread().getName());
            }
        }
    }

    public boolean close() {
        boolean result = true;
        if (jedis != null) {
            try {
                if (!jedis.getClient().isBroken()) {
                    jedis.close();
                }
                jedis = null;
                result = true;
            } catch (JedisConnectionException e) {
                logger.warn("jedis close exception", e);
                result = false;
            }
        }
        return result;
    }

    private void switchAddress() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setTestOnBorrow(true);
        InetSocketAddress address = this.addresses.get(random.nextInt(this.addresses.size()));
        this.host = address.getHostName() + ":" + address.getPort();
        log.info("databusSub switchAddress ip:{}, port:{}", address.getHostName(), address.getPort());
    }

    public void commitOffset(String partition, String offset) throws Exception {
        commitOffset(Integer.valueOf(partition), Long.valueOf(offset));
    }

    public void commitOffset(Integer partition, Long offset) throws Exception {
        try {
            if (!offset.equals(lastCommitOffset.get(partition)) && jedis != null) {//防止重复提交offset
                try {
                    lock.lock();
                    jedis.set(String.valueOf(partition), String.valueOf(offset));
                    lastCommitOffset.put(partition, offset);
                    log.info("partition " + partition + " has commit offset " + offset);
                } finally {
                    lock.unlock();
                }
            }
        } catch (JedisException e) {
            String msg = DataBuses.messageFromEx(e);
            log.warn("Commit consume connection error ! topic({}) group({}) message({})", topic, groupId, msg);
            DatabusClient.DATABUS_CLIENT_CODE.inc(groupId, host, "sub", msg);
        }
    }

    public List<DataBusMsgInfo> pollWithRetry() throws Exception {
        List<DataBusMsgInfo> result = Lists.newArrayList();
        try {
			result = poll();
        } catch (JedisException e) {
            String msg = DataBuses.messageFromEx(e);
            log.warn("Poll consume connection error ! topic({}) group({}) message({})", topic, groupId, msg);
            DatabusClient.DATABUS_CLIENT_CODE.inc(groupId, host, "sub", msg);
            reConnect();
        } catch (Exception e) {
            log.error("consume unexpected error ! ", e);
            DatabusClient.DATABUS_CLIENT_CODE.inc(groupId, host, "sub", "unexpected error");
        }
        return result;
    }

    public List<DataBusMsgInfo> poll() throws Exception{
		List<DataBusMsgInfo> result = Lists.newArrayList();
		List<String> res = Lists.newArrayList();
		try {
			lock.lock();
			if (jedis != null) {
				res = jedis.mget(topic);

			}
		} finally {
			lock.unlock();
		}
		if (res == null || res.size() < 1) {
			log.info("consume empty message, topic({}) group({})", topic, groupId);
			Thread.sleep(1_000);
			return null;
		}
		long start = System.nanoTime();
		try {
			List<JSONObject> list = res.stream().map(JSONObject::parseObject).collect(Collectors.toList());
			/**
			 * 这里一定要保证消费完成后  再set partition
			 * 保证消费成功
			 */
			for (JSONObject r : list) {
				log.debug("partition:{}, offset:{}. value:{}",r.getInteger("partition"), r.getInteger("offset"), r);
				try {
					result.add(getDatabusMsgInfo(r));
				} catch (Exception e) {
					log.error("consume accept message error ! topic({}) group({})", topic, groupId, e);
					DatabusClient.DATABUS_CLIENT_CODE.inc(groupId, host, "sub", "unexpected error");
					break;
				}
			}
		} finally {
			DatabusClient.DATABUS_CLIENT_DURATION.observe(
				BiliSimpleCollector.escapeMillisFromNanos(start, System.nanoTime()),
				groupId,
				host,
				"sub"
			);
		}
		return result;
	}

    private DataBusMsgInfo getDatabusMsgInfo(JSONObject r) {
        String metadata = r.getString("metadata");
        Integer partition = r.getInteger("partition");
        Long offset = r.getLong("offset");
        String topic = r.getString("topic");
        String key = r.getString("key");
        Object value = r.get("value");
        Integer timestamp = r.getInteger("timestamp");
        if (r.get("value") instanceof JSONObject || r.get("value") instanceof JSONArray) {
            return DataBusMsgInfo.builder().metadata(metadata).partition(partition).offset(offset)
                    .topic(topic).key(key).value(value).timestamp(timestamp).build();
        } else {
            log.error("不支持的格式类型! :{}", r.get("value"));
            throw new IllegalStateException("不支持的格式类型");
        }
    }

    private Jedis getNewRedisClient() {
        InetSocketAddress address = this.addresses.get(random.nextInt(this.addresses.size()));
        Jedis jedis = new Jedis(address.getHostName(), address.getPort(), 50 * 1000);
        jedis.auth(this.password);
        return jedis;
    }

    public boolean validateObject() {
        try {
            return jedis.isConnected() && jedis.ping().equals("PONG") && !jedis.getClient().isBroken();
        } catch (final Exception e) {
            return false;
        }
    }
}
