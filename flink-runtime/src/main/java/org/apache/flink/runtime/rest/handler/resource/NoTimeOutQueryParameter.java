package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

public class NoTimeOutQueryParameter extends MessageQueryParameter<Boolean> {

    private static final String KEY = "noTimeOut";

    private static final String DESCRIPTION = "The allocated resources should not be released by time out check.";

    public NoTimeOutQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public Boolean convertStringToValue(String value) throws ConversionException {
        return Boolean.parseBoolean(value);
    }

    @Override
    public String convertValueToString(Boolean value) {
        return value.toString();
    }
}
