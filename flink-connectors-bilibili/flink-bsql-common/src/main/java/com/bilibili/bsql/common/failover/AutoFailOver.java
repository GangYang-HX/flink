package com.bilibili.bsql.common.failover;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/17 6:34 下午
 */
public interface AutoFailOver<T, R> {


	void init();

	T selectSinkIndex(R index);

	void failedIndexRecord(R index, Boolean isSuccess);

	T failOver(R index);

	T failBack(R index);

	T normal(R index);


}
