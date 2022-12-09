package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

public class JobIDQueryParameter extends MessageQueryParameter<JobID> {
    private static final String KEY = "jobId";

    protected JobIDQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return "Optional job ID to attach the allocated resource to the specified job.";
    }

    @Override
    public JobID convertStringToValue(String value) throws ConversionException {
        return JobID.fromHexString(value);
    }

    @Override
    public String convertValueToString(JobID value) {
        return value.toString();
    }
}
