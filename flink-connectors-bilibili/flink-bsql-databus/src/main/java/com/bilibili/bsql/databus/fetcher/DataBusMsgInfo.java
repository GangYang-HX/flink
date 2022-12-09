package com.bilibili.bsql.databus.fetcher;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DatabusMsgInfo.java
 * @description This is the description of DatabusMsgInfo.java
 * @createTime 2020-10-22 18:28:00
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataBusMsgInfo implements Serializable {
	private static final long serialVersionUID = -3744367099366210562L;
	private String metadata;
	private Integer partition;
	private Long offset;
	private String topic;
	private String key;
	/**
	 * value数据类型为JSONObject或JSONArray
	 */
	private Object value;
	private Integer timestamp;
}
