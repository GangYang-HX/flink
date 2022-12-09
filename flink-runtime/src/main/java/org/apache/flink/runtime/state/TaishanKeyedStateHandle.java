package org.apache.flink.runtime.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class TaishanKeyedStateHandle implements KeyedStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(TaishanKeyedStateHandle.class);

	private final KeyGroupRange keyGroupRange;
	private final StreamStateHandle metaStateHandle;
	private final String taishanTableName;
	private final String accessToken;
	private final long checkpointId;
	private final String taishanSnapshotId;
	private final String cluster;
	private final String groupToken;
	private final String groupName;
	private final String appName;

	public TaishanKeyedStateHandle(KeyGroupRange keyGroupRange,
								   StreamStateHandle metaStateHandle,
								   String taishanTableName,
								   String accessToken,
								   long checkpointId,
								   String taishanSnapshotId,
								   String cluster,
								   String groupToken,
								   String groupName,
								   String appName) {
		this.keyGroupRange = keyGroupRange;
		this.metaStateHandle = metaStateHandle;
		this.taishanTableName = taishanTableName;
		this.accessToken = accessToken;
		this.checkpointId = checkpointId;
		this.taishanSnapshotId = taishanSnapshotId;
		this.cluster = cluster;
		this.groupToken = groupToken;
		this.groupName = groupName;
		this.appName = appName;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {

	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
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

	public StreamStateHandle getMetaStateHandle() {
		return metaStateHandle;
	}

	@Nullable
	@Override
	public KeyedStateHandle getIntersection(KeyGroupRange otherKeyGroupRange) {
		return this.keyGroupRange.getIntersection(otherKeyGroupRange).getNumberOfKeyGroups() > 0 ? this : null;
	}

	@Override
	public void discardState() throws Exception {
		try {
			metaStateHandle.discardState();
		} catch (Exception e) {
			LOG.warn("Could not properly discard meta data.", e);
		}
	}

	public String getTaishanSnapshotId() {
		return taishanSnapshotId;
	}

	public String getAccessToken() {
		return accessToken;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public String getTaishanTableName() {
		return taishanTableName;
	}

	@Override
	public long getStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		return size;
	}

	@Override
	public String toString() {
		return "TaishanKeyedStateHandle{" +
			"keyGroupRange=" + keyGroupRange +
			", metaStateHandle=" + metaStateHandle +
			", taishanTableName='" + taishanTableName + '\'' +
			", accessToken='" + accessToken + '\'' +
			", checkpointId=" + checkpointId +
			", taishanSnapshotId='" + taishanSnapshotId + '\'' +
			", cluster='" + cluster + '\'' +
			", groupToken='" + groupToken + '\'' +
			", groupName='" + groupName + '\'' +
			", appName='" + appName + '\'' +
			'}';
	}
}
