package org.apache.flink.taishan.state.restore;

import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.taishan.state.client.TaishanAdminConfiguration;

/**
 * @author Dove
 * @Date 2022/6/20 4:50 下午
 */
public interface TaishanRestoreOperation extends RestoreOperation<TaishanRestoreResult> {
	@Override
	TaishanRestoreResult restore() throws Exception;

	TaishanAdminConfiguration getTaishanAdminConfiguration();
}
