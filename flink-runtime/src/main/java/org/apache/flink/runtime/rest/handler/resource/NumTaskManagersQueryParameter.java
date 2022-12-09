package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

/**
 * Optional query parameter specifying the number of task managers to request.
 */
public class NumTaskManagersQueryParameter extends MessageQueryParameter<Integer> {

    private static final String NUM_TASK_MANAGERS = "numTaskManagers";

    public NumTaskManagersQueryParameter() {
        super(NUM_TASK_MANAGERS, MessageParameter.MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return "String value that specifies the number of task managers to request.";
    }

    @Override
    public Integer convertStringToValue(String value) throws ConversionException {
        return Integer.valueOf(value);
    }

    @Override
    public String convertValueToString(Integer value) {
        return value.toString();
    }
}
