package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

public class ResourcesCanBeSharedQueryParameter extends MessageQueryParameter<Boolean> {

    private static final String KEY = "resourcesCanBeShared";

    public ResourcesCanBeSharedQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return "The allocated resources can be shared or not.";
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
