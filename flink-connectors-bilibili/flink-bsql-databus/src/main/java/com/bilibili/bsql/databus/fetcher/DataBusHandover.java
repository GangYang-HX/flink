package com.bilibili.bsql.databus.fetcher;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * 数据中转
 *
 * @author weiximing
 * @version 1.0.0
 * @className DataBusHandover.java
 * @description This is the description of DataBusHandover.java
 * @createTime 2020-10-22 18:27:00
 */
public class DataBusHandover {
    public static final String FLINK_DATABUS_DIFF = "FLINK_DATABUS_DIFF";

    private final LinkedBlockingQueue<DataBusMsgInfo> queue = new LinkedBlockingQueue<>(2000);
    private final int index;//当前并行度位置
    private final int num;//消费总并行度数量
    private final Counter diff;//消息堆积情况

    public DataBusHandover(StreamingRuntimeContext runtimeContext) {
        this.num = runtimeContext.getNumberOfParallelSubtasks();
        this.index = runtimeContext.getIndexOfThisSubtask();
        this.diff = runtimeContext.getMetricGroup().counter(FLINK_DATABUS_DIFF);
    }

    public DataBusMsgInfo pollNext() throws InterruptedException {
        DataBusMsgInfo result = null;
        if (!queue.isEmpty()) {
            result = queue.take();
            diff.dec();
            return result;
        } else{
        	//避免cpu被打爆 每秒最高1000
        	Thread.sleep(1);
		}
        return result;
    }

    public void produce(List<DataBusMsgInfo> databusMsgInfoList) throws InterruptedException {
        if (databusMsgInfoList != null) {
            for (DataBusMsgInfo databusMsg : databusMsgInfoList) {
                queue.put(databusMsg);
                diff.inc();
            }
        }
    }
}
