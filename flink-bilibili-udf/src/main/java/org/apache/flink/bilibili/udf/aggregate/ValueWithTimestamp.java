package org.apache.flink.bilibili.udf.aggregate;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author zhangyang
 * @Date:2020/4/20
 * @Time:5:49 PM
 */
public class ValueWithTimestamp<T> implements Serializable {

    private Long eventTime;
    private T    value;

    public ValueWithTimestamp() {
    }

    public ValueWithTimestamp(Long eventTime, T value) {
        this.eventTime = eventTime;
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "ValueWithTimestamp{" +
                "eventTime=" + eventTime +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueWithTimestamp<?> that = (ValueWithTimestamp<?>) o;
        return Objects.equals(eventTime, that.eventTime) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime, value);
    }
}
