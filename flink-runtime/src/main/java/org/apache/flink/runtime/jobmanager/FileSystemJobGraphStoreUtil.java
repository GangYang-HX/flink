package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;

public enum FileSystemJobGraphStoreUtil implements JobGraphStoreUtil {
    INSTANCE;

    @Override
    public String jobIDToName(JobID jobId) {
        return String.format("/%s", jobId.toString());
    }

    @Override
    public JobID nameToJobID(String name) {
        return JobID.fromHexString(name.substring(name.lastIndexOf("/") + 1));
    }
}
