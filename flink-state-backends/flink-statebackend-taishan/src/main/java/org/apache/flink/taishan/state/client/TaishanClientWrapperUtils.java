package org.apache.flink.taishan.state.client;

import com.bilibili.taishan.ClientManager;
import com.bilibili.taishan.TaishanClient;
import com.bilibili.taishan.exception.TaishanException;

import java.io.IOException;

import com.bilibili.taishan.AccessOptions;

/**
 * @author Dove
 * @Date 2022/6/20 2:36 下午
 */
public class TaishanClientWrapperUtils {

	public static TaishanClientWrapper openTaishanClient(ClientManager clientManager, TaishanClientConfiguration configuration, String tableName, String accessToken) throws IOException {
		AccessOptions accessOptions = TaishanClientManager.getAccessOptions(configuration);
		accessOptions.setToken(accessToken);
		TaishanClient taishanClient;
		try {
			taishanClient = clientManager.openTable(tableName, accessOptions);
		} catch (TaishanException e) {
			// todo code: NET_CONNECTION_ERROR[1005], message: all metaserver tries to get table dist failed
			// todo code: INVALID_PARAM[1003], message: invalid token for table
			throw new IOException("Error while opening Taishan instance(" + tableName + "), token is " + accessToken, e);
		}
		long scanSleepTimeBase = configuration.getScanSleepTimeBase();
		return new TaishanClientWrapper(taishanClient, tableName, scanSleepTimeBase);
	}

}
