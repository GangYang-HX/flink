package org.apache.flink.taishan.state.restore;

import com.bilibili.taishan.ClientManager;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.taishan.state.client.TaishanAdminConfiguration;

import java.util.LinkedHashMap;

/**
 * @author Dove
 * @Date 2022/6/20 4:56 下午
 */
public class TaishanNoneRestoreOperation<K> extends AbstractTaishanRestoreOperation<K> {
	private final TaishanAdminConfiguration taishanAdminConfiguration;

	public TaishanNoneRestoreOperation(ClientManager clientManager,
									   ClassLoader userCodeClassLoader,
									   StateSerializerProvider<K> keySerializerProvider,
									   LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation,
									   TaishanAdminConfiguration taishanAdminConfiguration) {
		super(clientManager, userCodeClassLoader, keySerializerProvider, kvStateInformation);
		this.taishanAdminConfiguration = taishanAdminConfiguration;
	}

	@Override
	public TaishanRestoreResult restore() throws Exception {
		// delay open client;
		return new TaishanRestoreResult(null, null, null);
	}

	@Override
	public TaishanAdminConfiguration getTaishanAdminConfiguration() {
		return taishanAdminConfiguration;
	}
}
