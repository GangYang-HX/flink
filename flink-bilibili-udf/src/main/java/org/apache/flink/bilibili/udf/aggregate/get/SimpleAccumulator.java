package org.apache.flink.bilibili.udf.aggregate.get;

/**
 * @author zhangyang
 * @Date:2020/6/17
 * @Time:2:47 PM
 */
public class SimpleAccumulator {

    private Long val = 0L;

    public void set(Object val) {
        if (val instanceof Long) {
            this.val = (Long) val;
            return;
        }
        try {
            this.val = Long.valueOf(String.valueOf(val));
        } catch (Exception e) {
            // ignore
            this.val = 0L;
        }
    }

    public Long get() {
        return val;
    }
}
