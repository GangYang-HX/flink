package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

public class RemoveHostsAfterRestartQueryParameter extends MessageQueryParameter<Boolean> {

    public static final String KEY = "removeHostsAfterRestart";

    public RemoveHostsAfterRestartQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public String getDescription() {
        return "Whether to remove the list of blacklisted machines after job restarted or failed, default is true.";
    }


	@Override
	public Boolean convertStringToValue(String value) throws ConversionException {
    	if (StringUtils.isEmpty(value)) {
    		return true;
		}
		return Boolean.valueOf(value);
	}

	@Override
	public String convertValueToString(Boolean value) {
		return String.valueOf(value);
	}
}
