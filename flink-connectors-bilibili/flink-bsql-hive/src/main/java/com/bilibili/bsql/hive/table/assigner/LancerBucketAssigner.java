package com.bilibili.bsql.hive.table.assigner;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class LancerBucketAssigner extends BiliBucketAssigner implements BucketIdGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(LancerBucketAssigner.class);
	private static final String BUCKET_TIME_KEY= "GATEWAY_RECEIVE_TIME";
	private static final String MILLISECONDS_PADDING = "000";

	private final int metaPos;
	private final String bucketTimeKey;

	public LancerBucketAssigner(String partitionBy, int eventTimePos, int metaPos, String bucketTimeKey, boolean allowDrift, boolean pathContainPartitionKey) {
		super(partitionBy, eventTimePos, allowDrift,  pathContainPartitionKey);
		this.metaPos = metaPos;
		this.bucketTimeKey = bucketTimeKey;
	}

	@Override
	public Long getBucketTime(Boolean useProcTime, Row element, int eventTimePos, Long wm) {
		// event time by metaFied
		Map<String,byte[]> metaMap = (Map<String,byte[]>) element.getField(metaPos);
		Long bucketTime = System.currentTimeMillis();
		if (StringUtils.isNotBlank(bucketTimeKey)) {
			String timeStampStr = new String(metaMap.get(bucketTimeKey));
			if (timeStampStr.length() == 10) {
				timeStampStr += MILLISECONDS_PADDING;
			}
			bucketTime = Long.parseLong(timeStampStr);
			LOG.info("LancerBucketAssigner bucketTimeKey : {},value : {}", bucketTimeKey, timeStampStr);
		} else if (metaMap.containsKey(BUCKET_TIME_KEY)) {
			LOG.info("LancerBucketAssigner bucketTimeKey : {}", BUCKET_TIME_KEY);
			bucketTime = Long.parseLong(new String(metaMap.get(BUCKET_TIME_KEY)));
		}
		// allow partition drift when eventTime < wm
		if (allowDrift && bucketTime < wm) {
			bucketTime = wm;
		}

		return bucketTime;
	}


}
