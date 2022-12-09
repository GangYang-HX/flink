package org.apache.flink.bilibili.starter;

import java.io.Serializable;

public class UserInfo implements Serializable {
    public String user_name;
    public int age;
    public int sex;
    public String order_id;
    public String movement;
    public String log_date;
    public String log_hour;
}
