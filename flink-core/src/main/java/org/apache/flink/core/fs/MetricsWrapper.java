package org.apache.flink.core.fs;

public interface MetricsWrapper{


	void recordOpenRt(long l);

	void recordFlushRt(long l);

	void recordCloseRt(long l);

}
