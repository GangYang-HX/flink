package org.apache.flink.bilibili.starter;

import java.io.Serializable;

public class OrderInfo implements Serializable {
    public String word;
    public long num;
    public String event_type;
    public String app_id;
    public String log_date;
    public String log_hour;
}
