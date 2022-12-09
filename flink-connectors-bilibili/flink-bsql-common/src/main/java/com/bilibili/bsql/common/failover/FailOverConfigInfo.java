package com.bilibili.bsql.common.failover;

import java.io.Serializable;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/18 2:15 下午
 */
public class FailOverConfigInfo implements Serializable {
	/**
	 * failOverRate
	 */
	private Double failOverRate;
	/**
	 * statistics time interval
	 */
	private Integer failOverTimeLength;
	/**
	 * failOver time out value
	 */
	private Integer baseFailBackInterval;
	/**
	 * failOver  Switch
	 */
	private Boolean failOverSwitchOn;
	/**
	 * failOver index max ratios
	 * calc : Sum(failedIndex)/Sum(allIndex)
	 */
	private Double maxFailedIndexRatio;

	public Double getFailOverRate() {
		return failOverRate;
	}

	public void setFailOverRate(Double failOverRate) {
		this.failOverRate = failOverRate;
	}

	public Integer getFailOverTimeLength() {
		return failOverTimeLength;
	}

	public void setFailOverTimeLength(Integer failOverTimeLength) {
		this.failOverTimeLength = failOverTimeLength;
	}

	public Integer getBaseFailBackInterval() {
		return baseFailBackInterval;
	}

	public void setBaseFailBackInterval(Integer baseFailBackInterval) {
		this.baseFailBackInterval = baseFailBackInterval;
	}

	public Boolean getFailOverSwitchOn() {
		return failOverSwitchOn;
	}

	public void setFailOverSwitchOn(Boolean failOverSwitchOn) {
		this.failOverSwitchOn = failOverSwitchOn;
	}

	public Double getMaxFailedIndexRatio() {
		return maxFailedIndexRatio;
	}

	public void setMaxFailedIndexRatio(Double maxFailedIndexRatio) {
		this.maxFailedIndexRatio = maxFailedIndexRatio;
	}


	public static final class FailOverConfigInfoBuilder {
		private Double failOverRate;
		private Integer failOverTimeLength;
		private Integer baseFailBackInterval;
		private Boolean failOverSwitchOn;
		private Double maxFailedIndexRatio;

		private FailOverConfigInfoBuilder() {
		}

		public static FailOverConfigInfoBuilder aFailOverConfigInfo() {
			return new FailOverConfigInfoBuilder();
		}

		public FailOverConfigInfoBuilder withFailOverRate(Double failOverRate) {
			this.failOverRate = failOverRate;
			return this;
		}

		public FailOverConfigInfoBuilder withFailOverTimeLength(Integer failOverTimeLength) {
			this.failOverTimeLength = failOverTimeLength;
			return this;
		}

		public FailOverConfigInfoBuilder withBaseFailBackInterval(Integer baseFailBackInterval) {
			this.baseFailBackInterval = baseFailBackInterval;
			return this;
		}

		public FailOverConfigInfoBuilder withFailOverSwitchOn(Boolean failOverSwitchOn) {
			this.failOverSwitchOn = failOverSwitchOn;
			return this;
		}

		public FailOverConfigInfoBuilder withMaxFailedIndexRatio(Double maxFailedIndexRatio) {
			this.maxFailedIndexRatio = maxFailedIndexRatio;
			return this;
		}

		public FailOverConfigInfo build() {
			FailOverConfigInfo failOverConfigInfo = new FailOverConfigInfo();
			failOverConfigInfo.setFailOverRate(failOverRate);
			failOverConfigInfo.setFailOverTimeLength(failOverTimeLength);
			failOverConfigInfo.setBaseFailBackInterval(baseFailBackInterval);
			failOverConfigInfo.setFailOverSwitchOn(failOverSwitchOn);
			failOverConfigInfo.setMaxFailedIndexRatio(maxFailedIndexRatio);
			return failOverConfigInfo;
		}
	}
}
