package org.apache.flink.taishan.state.restore;

import com.bilibili.taishan.ClientManager;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.taishan.state.client.TaishanClientConfiguration;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.taishan.state.client.TaishanClientWrapperUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * @author Dove
 * @Date 2022/6/20 4:52 下午
 */
public abstract class AbstractTaishanRestoreOperation<K> implements TaishanRestoreOperation, AutoCloseable {
	protected ClientManager clientManager;
	protected TaishanClientWrapper taishanClientWrapper;
	protected final ClassLoader userCodeClassLoader;
	protected boolean isKeySerializerCompatibilityChecked;
	protected final StateSerializerProvider<K> keySerializerProvider;
	protected final LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation;


	protected AbstractTaishanRestoreOperation(ClientManager clientManager,
											  ClassLoader userCodeClassLoader,
											  StateSerializerProvider<K> keySerializerProvider,
											  LinkedHashMap<String, RegisteredStateMetaInfoBase> kvStateInformation) {
		this.clientManager = clientManager;
		this.userCodeClassLoader = userCodeClassLoader;
		this.keySerializerProvider = keySerializerProvider;
		this.kvStateInformation = kvStateInformation;
	}

	void openClient(String taishanTableName, String accessToken, TaishanClientConfiguration configuration) throws IOException {
		taishanClientWrapper = TaishanClientWrapperUtils.openTaishanClient(clientManager, configuration, taishanTableName, accessToken);
	}

	public TaishanClientWrapper getTaishanClientWrapper() {
		return this.taishanClientWrapper;
	}

	@Override
	public void close() throws Exception {
		if (clientManager != null) {
			clientManager.destroy();
		}

		IOUtils.closeQuietly(taishanClientWrapper);
	}

	KeyedBackendSerializationProxy<K> readMetaData(DataInputView dataInputView)
		throws IOException, StateMigrationException {
		// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
		// deserialization of state happens lazily during runtime; we depend on the fact
		// that the new serializer for states could be compatible, and therefore the restore can continue
		// without old serializers required to be present.
		KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(userCodeClassLoader);
		serializationProxy.read(dataInputView);
		if (!isKeySerializerCompatibilityChecked) {
			// check for key serializer compatibility; this also reconfigures the
			// key serializer to be compatible, if it is required and is possible
			TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
				keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(serializationProxy.getKeySerializerSnapshot());
			if (keySerializerSchemaCompat.isCompatibleAfterMigration() || keySerializerSchemaCompat.isIncompatible()) {
				throw new StateMigrationException("The new key serializer must be compatible.");
			}

			isKeySerializerCompatibilityChecked = true;
		}

		return serializationProxy;
	}

	// when restore, this method need to be thread safe
	synchronized RegisteredStateMetaInfoBase getOrRegisterStateColumnFamilyHandle(
		StateMetaInfoSnapshot stateMetaInfoSnapshot) {

		RegisteredStateMetaInfoBase registeredStateMetaInfoEntry =
			kvStateInformation.get(stateMetaInfoSnapshot.getName());

		if (null == registeredStateMetaInfoEntry) {
			// create a meta info for the state on restore;
			// this allows us to retain the state in future snapshots even if it wasn't accessed
			RegisteredStateMetaInfoBase stateMetaInfo =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
			kvStateInformation.put(stateMetaInfoSnapshot.getName(), stateMetaInfo);
		} else {
			// TODO with eager state registration in place, check here for serializer migration strategies
		}
		return registeredStateMetaInfoEntry;
	}
}
