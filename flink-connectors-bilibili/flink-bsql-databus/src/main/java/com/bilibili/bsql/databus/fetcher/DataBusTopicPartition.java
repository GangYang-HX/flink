package com.bilibili.bsql.databus.fetcher;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DataBusTopicPartition.java
 * @description This is the description of DataBusTopicPartition.java
 * @createTime 2020-10-22 18:39:00
 */
public class DataBusTopicPartition implements Serializable {
	private final String topic;
	private final int partition;
	private final int cachedHash;

	public DataBusTopicPartition(String topic, int partition) {
		this.topic = (String) Objects.requireNonNull(topic);
		this.partition = partition;
		this.cachedHash = 31 * topic.hashCode() + partition;
	}

	public String getTopic() {
		return this.topic;
	}

	public int getPartition() {
		return this.partition;
	}

	public String toString() {
		return "DatabusTopicPartition{topic='" + this.topic + '\'' + ", partition=" + this.partition + '}';
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (!(o instanceof DataBusTopicPartition)) {
			return false;
		} else {
			DataBusTopicPartition that = (DataBusTopicPartition) o;
			return this.partition == that.partition && this.topic.equals(that.topic);
		}
	}

	public int hashCode() {
		return this.cachedHash;
	}

	public static String toString(Map<DataBusTopicPartition, Long> map) {
		StringBuilder sb = new StringBuilder();
		Iterator var2 = map.entrySet().iterator();

		while (var2.hasNext()) {
			Map.Entry<DataBusTopicPartition, Long> p = (Map.Entry) var2.next();
			DataBusTopicPartition ktp = (DataBusTopicPartition) p.getKey();
			sb.append(ktp.getTopic()).append(":").append(ktp.getPartition()).append("=").append(p.getValue()).append(", ");
		}
		return sb.toString();
	}

	public static String toString(List<DataBusTopicPartition> partitions) {
		StringBuilder sb = new StringBuilder();
		Iterator var2 = partitions.iterator();
		while (var2.hasNext()) {
			DataBusTopicPartition p = (DataBusTopicPartition) var2.next();
			sb.append(p.getTopic()).append(":").append(p.getPartition()).append(", ");
		}
		return sb.toString();
	}
}
