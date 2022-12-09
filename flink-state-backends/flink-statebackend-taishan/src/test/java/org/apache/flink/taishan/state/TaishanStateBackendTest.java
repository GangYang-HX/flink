package org.apache.flink.taishan.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * @author Dove
 * @Date 2022/7/11 2:50 下午
 */

@RunWith(Parameterized.class)
public class TaishanStateBackendTest extends StateBackendTestBase<TaishanStateBackend> {

	@Parameterized.Parameters(name = "heapType: {0}")
	public static List<String> modes() {
		return Arrays.asList("none", "heap", "offheap");
	}

	@Parameterized.Parameter
	public String heapType;

	public TaishanStateBackend backend;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected TaishanStateBackend getStateBackend() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		TaishanStateBackend taishanStateBackend = new TaishanStateBackend(checkpointPath);
		Configuration configuration = new Configuration();
		configuration.setString("taishan.meta.addresses", "172.23.34.13:7200,172.23.34.22:7200,172.23.34.12:7200");
//		env.getConfig().setIdleStateRetentionTime(Time.minutes(10), Time.minutes(15)); // 关闭 ttl
		configuration.setString("taishan.group.token", "ad5253da4c779818");
		configuration.setString("taishan.group.name", "datacenter.flink");
		configuration.setString("taishan.cluster", "sh001");
		configuration.setString("taishan.root.url", "http://uat-taishan-admin.bilibili.co");
		configuration.setString("state.backend.cache.type", heapType);
		configuration.setInteger("taishan.client.timeout", 60000);

		if (backend != null) {
			closeBackend(backend);
		}
		backend = taishanStateBackend.configure(configuration, Thread.currentThread().getContextClassLoader());
		return backend;
	}

	@After
	public void close() {
		closeBackend(backend);
	}

	private void closeBackend(TaishanStateBackend closeBackend) {
//		closeBackend;
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return false;
	}
}
