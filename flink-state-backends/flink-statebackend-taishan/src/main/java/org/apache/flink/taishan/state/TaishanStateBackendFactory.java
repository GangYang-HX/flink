package org.apache.flink.taishan.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

import java.io.IOException;

/**
 * @author Dove
 * @Date 2022/6/30 3:51 下午
 */
public class TaishanStateBackendFactory implements StateBackendFactory<TaishanStateBackend> {

	@Override
	public TaishanStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
		throws IllegalConfigurationException, IOException {

		// we need to explicitly read the checkpoint directory here, because that
		// is a required constructor parameter
		final String checkpointDirURI = config.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
		if (checkpointDirURI == null) {
			throw new IllegalConfigurationException(
				"Cannot create the Taishan state backend: The configuration does not specify the " +
					"checkpoint directory '" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + '\'');
		}

		return new TaishanStateBackend(checkpointDirURI).configure(config, classLoader);
	}
}
