package com.bilibili.bsql.hbase.util;

import com.netflix.hystrix.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhuzhengjun
 * @date 2020/11/12 1:15 下午
 */
public class PrimaryCommand extends HystrixCommand<Result> {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryCommand.class);

    private Connection table;
    private Get getRequest;
    private TableName tableName;
    private Table workingTable;


    public PrimaryCommand(String primaryKey){
        this(null,null,null,primaryKey);
    }

    public PrimaryCommand(Connection table,TableName tableName,
                          Get getRequest,String primaryKey) {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("bili"))
                .andCommandKey(HystrixCommandKey.Factory.asKey(primaryKey))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(primaryKey))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withCoreSize(20)    //   定义了线程池的大小    netflix 大部分设置为10，极小一部分设为25
                                .withMaximumSize(200)
                )
                .andCommandPropertiesDefaults(
                        HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(2000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(10000000)
                                .withMetricsHealthSnapshotIntervalInMilliseconds(200)
                                .withMetricsRollingStatisticalWindowInMilliseconds(3000)
                                .withMetricsRollingStatisticalWindowBuckets(10)
                                .withMetricsRollingPercentileWindowInMilliseconds(10000)
                                .withMetricsRollingPercentileWindowBuckets(10)
                                .withCircuitBreakerErrorThresholdPercentage(10)
                                .withCircuitBreakerEnabled(false)
                                .withRequestCacheEnabled(false)
                )
        );

        this.table = table;
        this.tableName = tableName;
        this.getRequest = getRequest;

    }

    @Override
    protected Result run() throws Exception{

        try {
            this.workingTable = table.getTable(tableName);
            Result r = workingTable.get(getRequest);

            return r;
        }catch (Exception e){
            throw e;
        }
        finally {
            if (this.workingTable!=null){
                this.workingTable.close();
            }
        }
    }

    @Override
    protected Result getFallback() {
        try {
            if (this.workingTable!=null){
                this.workingTable.close();
            }
        }catch (Exception e){
            //todo: add some failure hint
        }

        //外部的异步靠这个Exception感知到hbase查询失败
        throw new HbaseQueryException(getRequest);
    }

    public static class HbaseQueryException extends RuntimeException{
        HbaseQueryException(Get getRequest){
            super(String.format("%s query error/timeout/threadpool too full",getRequest));
        }
    }

}

