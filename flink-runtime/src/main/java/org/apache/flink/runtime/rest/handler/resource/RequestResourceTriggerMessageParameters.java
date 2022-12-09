package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class RequestResourceTriggerMessageParameters extends MessageParameters {

    public JobIDQueryParameter jobId = new JobIDQueryParameter();

    public NumSlotsQueryParameter numSlots = new NumSlotsQueryParameter();

    public NumTaskManagersQueryParameter numTaskManagers = new NumTaskManagersQueryParameter();

    public ResourcesCanBeSharedQueryParameter resourcesCanBeShared = new ResourcesCanBeSharedQueryParameter();

    public NoTimeOutQueryParameter noTimeOut = new NoTimeOutQueryParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.emptyList();
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        jobId,
                        numSlots,
                        numTaskManagers,
                        resourcesCanBeShared,
                        noTimeOut));
    }
}
