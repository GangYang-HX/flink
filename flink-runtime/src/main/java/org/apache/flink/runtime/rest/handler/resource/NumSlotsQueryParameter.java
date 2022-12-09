package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

/**
 * Optional query parameter specifying the number of slots to request.
 */
public class NumSlotsQueryParameter extends MessageQueryParameter<Integer> {
    private static final String NUM_SLOTS = "numSlots";

    protected NumSlotsQueryParameter() {
        super(NUM_SLOTS, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return "Number of slots to request (with the session or job's default resource profile).";
    }

    @Override
    public Integer convertStringToValue(String value) throws ConversionException {
        return Integer.parseInt(value);
    }

    @Override
    public String convertValueToString(Integer value) {
        return value.toString();
    }
}
