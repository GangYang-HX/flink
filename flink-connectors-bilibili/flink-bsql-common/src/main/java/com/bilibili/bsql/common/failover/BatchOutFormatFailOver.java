package com.bilibili.bsql.common.failover;

import com.bilibili.bsql.common.utils.DateUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/17 8:29 下午
 */
@NotThreadSafe
public class BatchOutFormatFailOver implements AutoFailOver<Integer, Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(BatchOutFormatFailOver.class);

	private final double failOverRate;

	private final int failoverTimeLength;

	private final boolean failOverSwitchOn;


	/**
	 * failOver flags
	 */
	private final List<Boolean> failoverFlag;

	/**
	 * failBack flags
	 */
	private final List<Boolean> failBackFlag;

	/**
	 * failBack circle
	 */
	private final List<Long> failBackTimestamp;


	private final List<long[]> failIndex;

	private final List<long[]> sumIndex;

	private final int allIndex;

	private final double maxFailedIndexRatio;


	private final long baseFailBackInterval;


	public BatchOutFormatFailOver(double failOverRate, int failoverTimeLength, long baseFailBackInterval, boolean failOverSwitchOn, final int allIndex, final double maxFailedIndexRatio) {

		this.failOverRate = failOverRate;
		this.failoverTimeLength = failoverTimeLength;
		failIndex = new ArrayList<>();
		sumIndex = new ArrayList<>();
		this.baseFailBackInterval = baseFailBackInterval;
		this.failOverSwitchOn = failOverSwitchOn;
		this.allIndex = allIndex;
		failoverFlag = new ArrayList<>(allIndex);
		failBackFlag = new ArrayList<>(allIndex);
		this.maxFailedIndexRatio = maxFailedIndexRatio;
		this.failBackTimestamp = new ArrayList<>(allIndex);
		for (int i = 0; i < allIndex; ++i) {
			long[] failLongs = new long[getArrayLength()];
			failIndex.add(failLongs);
			long[] longs = new long[getArrayLength()];
			sumIndex.add(longs);
			failoverFlag.add(false);
			failBackFlag.add(false);
			failBackTimestamp.add(0L);
		}

	}


	@Override
	public void init() {

	}

	public int getAllIndex() {
		return allIndex;
	}

	@Override
	public void failedIndexRecord(Integer index, Boolean isSuccess) {
		long nowTime = Clock.systemUTC().millis();
		if (!isSuccess) {
			incrementCount(failIndex.get(index)
				, 1, nowTime);
			failIndex.get(index)[getDataCleanIndex(nowTime)] = 0;
		}
		incrementCount(sumIndex.get(index), 1, nowTime);
		sumIndex.get(index)[getDataCleanIndex(nowTime)] = 0;

	}

	private void incrementCount(long[] array, long num, long timestamp) {
		int index = getArrayIndex(timestamp);
		array[index] += num;
	}

	/**
	 * get circular array index
	 *
	 * @param timestamp timestamp
	 * @return return circular array index
	 */
	private int getArrayIndex(long timestamp) {
		return DateUtils.getCurrentMinuteOfHour(timestamp).intValue() % getArrayLength();
	}

	/**
	 * get clean index
	 *
	 * @param nowTime timestamp
	 * @return return index
	 */
	private int getDataCleanIndex(long nowTime) {
		return getArrayIndex(nowTime + DateUtils.MILLISECONDS_ONE_MINUTE);
	}

	private int getArrayLength() {
		return failoverTimeLength + 1;
	}

	@Override
	public Integer selectSinkIndex(Integer index) {
		if (!failOverSwitchOn) {
			return index;
		}

		long nowTime = Clock.systemUTC().millis();
		if (failoverFlag.get(index)) {
			if (nowTime > failBackTimestamp.get(index)) {
				//当前没有尝试failBack逻辑，timestamp超出直接切回原来的index
				LOG.info("client index :{} timestamp has timeout!", index);
				failBackFlag.set(index, true);
				failoverFlag.set(index, false);
				return failBack(index);
			}
			return failOver(index);
		} else {
			if (needFailOver(index, nowTime)) {
				refreshFailBackTime(index, nowTime);
				return failOver(index);
			}
			return normal(index);
		}
	}

	private void refreshFailBackTime(int k, long nowTime) {
		failBackTimestamp.set(k, nowTime + baseFailBackInterval);
	}

	private boolean needFailOver(final int k, long nowTime) {

		long failCount = 0;
		long sumCount = 0;
		int tmp = getDataCleanIndex(nowTime);
		for (int i = 0; i < getArrayLength(); ++i) {
			if (i == tmp) {
				continue;
			}
			failCount += failIndex.get(k)[i];
			sumCount += sumIndex.get(k)[i];
		}
		if (sumCount == 0) {
			return false;
		}
		if ((double) failCount / sumCount < failOverRate) {
			return false;
		}
		long failed = failoverFlag.stream().filter(e -> e).count();
		if ((double) failed / failoverFlag.size() > maxFailedIndexRatio) {
			LOG.error("failed index exceed ratio:{}", maxFailedIndexRatio);
			return false;
		}
		LOG.info("client index :{} has do failOver!", k);
		return true;
	}

	@Override
	public Integer failOver(Integer index) {
		failoverFlag.set(index, true);
		List<Integer> availableList = new ArrayList<>();
		for (int i = 0; i < failoverFlag.size(); ++i) {
			if (!failoverFlag.get(i)) {
				availableList.add(i);
			}
		}
		//prevent index out of exception
		if (!availableList.isEmpty()) {
			int tmp = new RandomDataGenerator().nextInt(0, availableList.size() - 1);
			return availableList.get(tmp);
		}
		return index;

	}

	@Override
	public Integer failBack(Integer index) {
		return index;
	}

	@Override
	public Integer normal(Integer index) {
		return index;
	}

}
