/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis.tableinfo;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.SinkTableInfo;
import com.bilibili.bsql.redis.RedisConstant;

import lombok.Data;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RedisSinkTableInfo.java, v 0.1 2020-10-28 19:12
zhouxiaogang Exp $$
 */
@Data
public class RedisSinkTableInfo extends SinkTableInfo implements Serializable {

    public static final String CURR_TYPE  = "redis";

    public static final String URL_KEY    = "url";
    public static final String TIMEOUT    = "timeout";
    public static final String MAXTOTAL   = "maxTotal";
    public static final String MAXIDLE    = "maxIdle";
    public static final String MINIDLE    = "minIdle";
    public static final String REDIS_TYPE = "redisType";
    public static final String EXPIRE     = "expire";

    public static final ConfigOption<String> REDIS_URL_KEY = ConfigOptions
            .key(URL_KEY)
            .stringType()
            .defaultValue("")
            .withDescription("redis url");

    public static final ConfigOption<Integer> REDIS_TIMEOUT = ConfigOptions
            .key(TIMEOUT)
            .intType()
            .defaultValue(0)
            .withDescription("redis timeout");

    public static final ConfigOption<String> MAXTOTAL_KEY = ConfigOptions
            .key(MAXTOTAL)
            .stringType()
            .noDefaultValue()
            .withDescription("redis max total");

    public static final ConfigOption<String> MAXIDLE_KEY = ConfigOptions
            .key(MAXIDLE)
            .stringType()
            .noDefaultValue()
            .withDescription("redis max idle");

    public static final ConfigOption<String> MINIDLE_KEY = ConfigOptions
            .key(MINIDLE)
            .stringType()
            .noDefaultValue()
            .withDescription("redis min idle");

    public static final ConfigOption<Integer> REDIS_TYPE_KEY = ConfigOptions
            .key(REDIS_TYPE)
            .intType()
            .noDefaultValue()
            .withDescription("redis timeout");

    public static final ConfigOption<Integer> EXPIRE_KEY = ConfigOptions
            .key(EXPIRE)
            .intType()
            .defaultValue(-1)
            .withDescription("redis timeout");

    private String url;
    private Integer timeout;
    private String maxTotal;
    private String maxIdle;
    private String minIdle;
    private Integer redisType;
    private Integer expire = -1;

    private Integer keyIndex;
    private Integer fieldIndex = -1;
    private Integer valueIndex;

    public RedisSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        setType(CURR_TYPE);
        this.url = helper.getOptions().get(REDIS_URL_KEY);
        this.timeout = helper.getOptions().get(REDIS_TIMEOUT);
        this.maxTotal = helper.getOptions().get(MAXTOTAL_KEY);
        this.maxIdle = helper.getOptions().get(MAXIDLE_KEY);
        this.minIdle = helper.getOptions().get(MINIDLE_KEY);
        this.redisType = helper.getOptions().get(REDIS_TYPE_KEY);
        this.expire = helper.getOptions().get(EXPIRE_KEY);

        checkArgument(StringUtils.isNotEmpty(url), "redis sink url can not be empty");
        checkArgument(redisType != null && redisType >= RedisConstant.CLUSTER,
                String.format("sink表:%s 只支持cluster和pipelined模式,redisType只能等于3或者4", getName()));
        checkArgument(physicalFields.size() == 2 || physicalFields.size() == 3,
                "redis has only k/v for now");
        checkArgument(getPrimaryKeyIdx().size() == 1 || getPrimaryKeyIdx().size() == 2,
                "redis only support for one key");

        this.keyIndex = getPrimaryKeyIdx().get(0);
        if (getPrimaryKeyIdx().size() == 2) {
            this.fieldIndex = getPrimaryKeyIdx().get(1);
        }
        for (int i = 0; i < getPhysicalFields().size(); i++){
            if (!getPrimaryKeyIdx().contains(i)) {
                this.valueIndex = i;
                break;
            }
        }
    }
}