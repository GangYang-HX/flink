package org.apache.flink.taishan.state.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.taishan.state.TaishanStateBackend;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.taishan.state.TaishanStateBackend.PriorityQueueStateType.HEAP;
import static org.apache.flink.taishan.state.TaishanStateBackend.PriorityQueueStateType.TAISHAN;

/**
 * @author Dove
 * @Date 2022/6/23 5:51 下午
 */
public class TaishanOptions {

	// java api client options
	public static final ConfigOption<String> TAISHAN_META_ADDRESSES =
		key("taishan.meta.addresses")
			.stringType()
			.noDefaultValue()
			.withDescription("Taishan meta addresses.");
	public static final ConfigOption<Integer> TAISHAN_CLIENT_TIMEOUT =
		key("taishan.client.timeout")
			.intType()
			.defaultValue(5000)
			.withDescription("Taishan client timeout in milliseconds.");
	public static final ConfigOption<Integer> TAISHAN_BACKUP_REQUEST_THREAD_POOL_SIZE =
		key("taishan.backup.request.thread.pool.size")
			.intType()
			.defaultValue(0)
			.withDescription("taishan.backup.request.thread.pool.size");
	public static final ConfigOption<Long> TAISHAN_SCAN_SLEEP_TIME_BASE =
		key("taishan.scan.sleep.time.base")
			.longType()
			.defaultValue(300L)
			.withDescription("taishan scan sleep time base");

	// http admin options
	public static final ConfigOption<String> TAISHAN_GROUP_TOKEN =
		key("taishan.group.token")
			.stringType()
			.defaultValue("efe7a0ff1c4cda07") // test: ad5253da4c779818
			.withDescription("Taishan group token.");

	public static final ConfigOption<String> TAISHAN_GROUP_NAME =
		key("taishan.group.name")
			.stringType()
			.defaultValue("datacenter.flink01") // test: datacenter.flink
			.withDescription("Taishan group name.");

	public static final ConfigOption<String> TAISHAN_APP_NAME =
		key("taishan.app.name")
			.stringType()
			.defaultValue("datacenter.hadoop.jssz-realtime-yarn-config")
			.withDescription("Taishan app name.");

	public static final ConfigOption<String> TAISHAN_ROOT_URL =
		key("taishan.root.url")
			.stringType()
			.defaultValue("http://taishan-admin.bilibili.co") // uat: http://uat-taishan-admin.bilibili.co
			.withDescription("Taishan root url.");

	public static final ConfigOption<String> TAISHAN_CLUSTER =
		key("taishan.cluster")
			.stringType()
			.defaultValue("csflink")// uat: sh001
			.withDescription("Taishan cluster.");

	public static final ConfigOption<Integer> TAISHAN_CREATE_TABLE_TIMEOUT =
		key("taishan.create.table.timeout")
			.intType()
			.defaultValue(3 * 60 * 1000)
			.withDescription("Taishan create table timeout in milliseconds.");

	// Backend configuration
	/**
	 * Choice of timer service implementation.
	 */
	public static final ConfigOption<TaishanStateBackend.PriorityQueueStateType> TIMER_SERVICE_FACTORY = ConfigOptions
		.key("state.backend.taishan.timer-service.factory")
		.enumType(TaishanStateBackend.PriorityQueueStateType.class)
		.defaultValue(TAISHAN)
		.withDescription(String.format("This determines the factory for timer service state implementation. Options " +
				"are either %s (heap-based) or %s for an implementation based on Taishan.",
			HEAP.name(), TAISHAN.name()));
}
