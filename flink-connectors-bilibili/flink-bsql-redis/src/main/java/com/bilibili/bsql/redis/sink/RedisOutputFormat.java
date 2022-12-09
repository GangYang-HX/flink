/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.bilibili.bsql.redis.sink;


import com.bilibili.bsql.common.format.BatchOutputFormat;
import com.bilibili.bsql.redis.sink.jedisbin.ClusterJedis;
import com.bilibili.bsql.redis.sink.jedisbin.JedisBinary;
import com.bilibili.bsql.redis.sink.jedisbin.JedisClusterPipeline;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * redis存储
 * 
 * @author zhouxiaogang
 */
public class RedisOutputFormat extends BatchOutputFormat {

    private String                  url;

    private int                     redisType;

    private String                  maxTotal;

    private String                  maxIdle;

    private String                  minIdle;

    protected DataType  fieldTypes;

    protected List<String>          primaryKeys;

    protected int                   timeout;

    protected int                   expire;

    private JedisBinary jedis;

    private GenericObjectPoolConfig poolConfig;

    private int                     keyIndex;

    private int                     fieldIndex;

    private int                     valIndex;

    private DynamicTableSink.DataStructureConverter converter;

    public RedisOutputFormat(DataType dataType, DynamicTableSink.Context context) {
        super(dataType,context);
    }

    @Override
    public String sinkType() {
        return "redis";
    }

    @Override
    public void doOpen(int taskNumber, int numTasks) throws IOException {
        super.doOpen(taskNumber, numTasks);
        establishConnection();
    }

    @Override
    public void doWrite(RowData record) {
        if (record.getRowKind() == RowKind.UPDATE_BEFORE || record.getRowKind() == RowKind.DELETE) {
            return;
        }

        Row row = (Row)converter.toExternal(record);
        if (row.getArity() != ((RowType)fieldTypes.getLogicalType()).getFieldCount()) {
            return;
        }

        Object redisKey = row.getField(keyIndex);
        Object redisField = null;
        if (fieldIndex != -1) {
            redisField = row.getField(fieldIndex);
        }
        Object redisValue = row.getField(valIndex);

        try {
            if (expire > 0) {
                if (redisField == null) {
                    jedis.setex(redisKey, expire, redisValue);
                } else {
                    jedis.hsetex(redisKey, expire, redisField, redisValue);
                }
            } else {
                if (redisField == null) {
                    jedis.set(redisKey, redisValue);
                } else {
                    jedis.hset(redisKey, redisField, redisValue);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {
        jedis.sync();
    }

    private GenericObjectPoolConfig setPoolConfig(String maxTotal, String maxIdle, String minIdle) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        if (maxTotal != null) {
            config.setMaxTotal(Integer.parseInt(maxTotal));
        }
        if (maxIdle != null) {
            config.setMaxIdle(Integer.parseInt(maxIdle));
        }
        if (minIdle != null) {
            config.setMinIdle(Integer.parseInt(minIdle));
        }
        config.setJmxEnabled(false);
        return config;
    }

    private void establishConnection() {
        poolConfig = setPoolConfig(maxTotal, maxIdle, minIdle);
        String[] nodes = url.split(",");
        Set<HostAndPort> addresses = new HashSet<>();
        Set<String> ipPorts = new HashSet<>();
        for (String ipPort : nodes) {
            ipPorts.add(ipPort);
            String[] ipPortPair = ipPort.split(":");
            addresses.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
        }
        if (timeout == 0) {
            timeout = 10000;
        }

        switch (redisType) {
            case 3:
                jedis = new ClusterJedis(new JedisCluster(addresses, timeout, 10, poolConfig));
                break;

            case 4:
                jedis = new JedisClusterPipeline(new JedisCluster(addresses, timeout, 10, poolConfig));
                break;
            default:
                throw new RuntimeException("不支持的redisType:" + redisType);
        }
    }

    @Override
    public void close() throws IOException {

        super.close();
        if (jedis != null) {
            jedis.close();
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getRedisType() {
        return redisType;
    }

    public void setRedisType(int redisType) {
        this.redisType = redisType;
    }

    public String getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(String maxTotal) {
        this.maxTotal = maxTotal;
    }

    public String getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(String maxIdle) {
        this.maxIdle = maxIdle;
    }

    public String getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(String minIdle) {
        this.minIdle = minIdle;
    }

    public DataType getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(DataType fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public JedisBinary getJedis() {
        return jedis;
    }

    public void setJedis(JedisBinary jedis) {
        this.jedis = jedis;
    }

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public int getKeyIndex() {
        return keyIndex;
    }

    public void setKeyIndex(int keyIndex) {
        this.keyIndex = keyIndex;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public void setFieldIndex(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    public int getValIndex() {
        return valIndex;
    }

    public void setValIndex(int valIndex) {
        this.valIndex = valIndex;
    }

    public DynamicTableSink.DataStructureConverter getConverter() {
        return converter;
    }

    public void setConverter(DynamicTableSink.DataStructureConverter converter) {
        this.converter = converter;
    }
}
