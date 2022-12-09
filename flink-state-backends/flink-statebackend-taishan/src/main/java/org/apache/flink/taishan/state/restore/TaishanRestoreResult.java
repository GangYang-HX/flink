package org.apache.flink.taishan.state.restore;

import org.apache.flink.taishan.state.client.TaishanClientWrapper;

/**
 * @author Dove
 * @Date 2022/6/20 4:51 下午
 */
public class TaishanRestoreResult {

	private TaishanClientWrapper taishanClientWrapper;
	private String taishanTableName;
	private String accessToken;

	public TaishanRestoreResult(TaishanClientWrapper taishanClientWrapper,
                                String taishanTableName,
								String accessToken) {
		this.taishanClientWrapper = taishanClientWrapper;
		this.taishanTableName = taishanTableName;
		this.accessToken = accessToken;
	}

	public TaishanClientWrapper getTaishanClientWrapper() {
		return taishanClientWrapper;
	}

	public void setTaishanClientWrapper(TaishanClientWrapper taishanClientWrapper) {
		this.taishanClientWrapper = taishanClientWrapper;
	}

	public String getAccessToken(){
		return accessToken;
	}

	public void setTaishanTableName(String taishanTableName) {
		this.taishanTableName = taishanTableName;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public String getTaishanTableName(){
		return taishanTableName;
	}

	@Override
	public String toString() {
		return "TaishanRestoreResult{" +
			"taishanTableName='" + taishanTableName + '\'' +
			", accessToken='" + accessToken + '\'' +
			'}';
	}
}
