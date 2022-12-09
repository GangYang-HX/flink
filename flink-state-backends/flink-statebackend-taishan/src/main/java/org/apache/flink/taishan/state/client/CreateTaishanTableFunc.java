package org.apache.flink.taishan.state.client;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Dove
 * @Date 2022/7/25 4:25 下午
 */
public class CreateTaishanTableFunc implements AggregateFunction<CreateTaishanTableFunc.AccInput, Map<String, String>, Map<String, String>> {
	public static final String NAME = CreateTaishanTableFunc.class.getSimpleName();
	private static final Logger LOG = LoggerFactory.getLogger(CreateTaishanTableFunc.class);

	// key: tableName, value: table token
	private static final Map<String, String> map = new HashMap<>();

	@Override
	public Map<String, String> createAccumulator() {
		return map;
	}

	@Override
	public Map<String, String> add(AccInput accInput, Map<String, String> accumulator) {
		int shardNum = accInput.getShardNum();
		String taishanTableName = accInput.getTaishanTableName();
		TaishanAdminConfiguration configuration = accInput.getConfiguration();

		String tableToken = accumulator.getOrDefault(taishanTableName, "");
		if (tableToken.isEmpty()) {
			LOG.info("Start creating the Taishan table:{}", taishanTableName);
			long createStart = System.currentTimeMillis();
			TaishanHttpUtils.ResponseBody responseBodyCreate = TaishanHttpUtils.createTaishanTable(configuration, taishanTableName, shardNum);
			long createEnd = System.currentTimeMillis();
			LOG.info("Create taishan table successed:{}, cost {}ms, create Body:{}", taishanTableName, createEnd - createStart, responseBodyCreate);

			String accessToken = responseBodyCreate.getAccessToken();
			accumulator.put(taishanTableName, accessToken);
		}
		return accumulator;
	}

	@Override
	public Map<String, String> getResult(Map<String, String> accumulator) {
		return accumulator;
	}

	@Override
	public Map<String, String> merge(Map<String, String> a, Map<String, String> b) {
		return null;
	}

	public static class AccInput implements Serializable {
		private String taishanTableName;
		private int shardNum;
		private TaishanAdminConfiguration configuration;

		public AccInput(String taishanTableName, int shardNum, TaishanAdminConfiguration configuration) {
			this.taishanTableName = taishanTableName;
			this.shardNum = shardNum;
			this.configuration = configuration;
		}

		public String getTaishanTableName() {
			return taishanTableName;
		}

		public int getShardNum() {
			return shardNum;
		}

		public TaishanAdminConfiguration getConfiguration() {
			return configuration;
		}
	}
}
