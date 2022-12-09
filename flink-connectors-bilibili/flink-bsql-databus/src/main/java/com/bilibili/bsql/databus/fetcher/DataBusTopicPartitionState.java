package com.bilibili.bsql.databus.fetcher;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DatabusTopicPartitionState.java
 * @description This is the description of DatabusTopicPartitionState.java
 * @createTime 2020-10-22 18:38:00
 */
public class DataBusTopicPartitionState<KPH> {
	private final DataBusTopicPartition partition;
	private final KPH kafkaPartitionHandle;
	private volatile long offset;
	private volatile long committedOffset;

	public DataBusTopicPartitionState(DataBusTopicPartition partition, KPH kafkaPartitionHandle) {
		this.partition = partition;
		this.kafkaPartitionHandle = kafkaPartitionHandle;
		this.offset = -915623761776L;
		this.committedOffset = -915623761776L;
	}

	public final DataBusTopicPartition getDatabusTopicPartition() {
		return this.partition;
	}

	public final KPH getDatabusPartitionHandle() {
		return this.kafkaPartitionHandle;
	}

	public final String getTopic() {
		return this.partition.getTopic();
	}

	public final int getPartition() {
		return this.partition.getPartition();
	}

	public final long getOffset() {
		return this.offset;
	}

	public final void setOffset(long offset) {
		this.offset = offset;
	}

	public final boolean isOffsetDefined() {
		return this.offset != -915623761776L;
	}

	public final void setCommittedOffset(long offset) {
		this.committedOffset = offset;
	}

	public final long getCommittedOffset() {
		return this.committedOffset;
	}

	public String toString() {
		return "Partition: " + this.partition + ", KafkaPartitionHandle=" + this.kafkaPartitionHandle + ", offset=" + (this.isOffsetDefined() ? String.valueOf(this.offset) : "(not set)");
	}
}
