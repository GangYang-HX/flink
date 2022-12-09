package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

public class HostsQueryParameter extends MessageQueryParameter<String> {

    public static final String KEY = "hosts";

    public HostsQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return "List of strings to specify the list of blacklisted machines.";
    }

    @Override
    public String convertStringToValue(String value) throws ConversionException {
        return value;
    }

    @Override
    public String convertValueToString(String value) {
        return value;
    }
}
