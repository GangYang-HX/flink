package org.apache.flink.taishan.state.client;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.TaishanKeyedStateHandle;
import org.apache.flink.taishan.state.configuration.TaishanOptions;

import java.io.Serializable;

/**
 * @author Dove
 * @Date 2022/8/10 2:06 下午
 */
public class TaishanAdminConfiguration implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final String CREATE_DROP_URL = "/taishan/api/external/table";
	private static final String QUERY_URL = "/taishan/api/external/table";
	private static final String SABER_JOB_ID = "SABER_JOB_ID";

	private String cluster;
	private String groupToken;
	private String groupName;
	private String appName;
	private String createDropUrl;
	private String queryUrl;
	private int createTableTimeOut;
	private final String saberJobId;

	public static TaishanAdminConfiguration loadAdminConfiguration(ReadableConfig config) {
		String cluster = config.get(TaishanOptions.TAISHAN_CLUSTER);
		String groupToken = config.get(TaishanOptions.TAISHAN_GROUP_TOKEN);
		String groupName = config.get(TaishanOptions.TAISHAN_GROUP_NAME);
		String appName = config.get(TaishanOptions.TAISHAN_APP_NAME);
		String taishanRootUrl = config.get(TaishanOptions.TAISHAN_ROOT_URL);
		int createTableTimeOut = config.get(TaishanOptions.TAISHAN_CREATE_TABLE_TIMEOUT);

		String createDropUrl = taishanRootUrl + CREATE_DROP_URL;
		String queryUrl = taishanRootUrl + QUERY_URL;
		String saberJobId = System.getProperty(SABER_JOB_ID);

		return new TaishanAdminConfiguration(cluster, groupToken, groupName, appName, createDropUrl, queryUrl, createTableTimeOut, saberJobId);
	}

	public static TaishanAdminConfiguration restoreAdminConfiguration(TaishanKeyedStateHandle taishanKeyedStateHandle) {
		String appName = taishanKeyedStateHandle.getAppName();
		String groupName = taishanKeyedStateHandle.getGroupName();
		String groupToken = taishanKeyedStateHandle.getGroupToken();
		String cluster = taishanKeyedStateHandle.getCluster();
		Configuration conf = new Configuration();
		String taishanRootUrl = conf.get(TaishanOptions.TAISHAN_ROOT_URL);
		int createTableTimeOut = conf.get(TaishanOptions.TAISHAN_CREATE_TABLE_TIMEOUT);
		String saberJobId = System.getProperty(SABER_JOB_ID);

		String createDropUrl = taishanRootUrl + CREATE_DROP_URL;
		String queryUrl = taishanRootUrl + QUERY_URL;
		return new TaishanAdminConfiguration(cluster, groupToken, groupName, appName, createDropUrl, queryUrl, createTableTimeOut, saberJobId);
	}

	private TaishanAdminConfiguration(String cluster, String groupToken, String groupName, String appName, String createDropUrl, String queryUrl, int createTableTimeOut, String saberJobId) {
		this.cluster = cluster;
		this.groupToken = groupToken;
		this.groupName = groupName;
		this.appName = appName;
		this.createDropUrl = createDropUrl;
		this.queryUrl = queryUrl;
		this.createTableTimeOut = createTableTimeOut;
		this.saberJobId = saberJobId;
	}

	public String getCluster() {
		return cluster;
	}

	public String getGroupToken() {
		return groupToken;
	}

	public String getGroupName() {
		return groupName;
	}

	public String getAppName() {
		return appName;
	}

	public String getCreateDropUrl() {
		return createDropUrl;
	}

	public String getQueryUrl() {
		return queryUrl;
	}

	public int getCreateTableTimeOut() {
		return createTableTimeOut;
	}

	public String getSaberJobId() {
		return saberJobId;
	}
}
