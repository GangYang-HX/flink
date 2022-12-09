package org.apache.flink.runtime.statistics.datatrace;

import org.apache.flink.runtime.statistics.AbstractOnceSender;
import org.apache.flink.runtime.statistics.StartupUtils;
import org.apache.flink.runtime.statistics.StatisticsUtils;

import com.bilibili.datatrace.client.tools.Tracer;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Map;

public class TraceOnceSender extends AbstractOnceSender {

    // Default use local log report event tracking.
    private static final String DEFAULT_REPORT_MODE = "LOCAL_LOG";

    private static final String CLIENT_APP = "task_trace";

    private static final String CLIENT_SOURCE = "flink_tracer";
    private Tracer tracer;
    private String traceID;

    public TraceOnceSender() {
        init();
    }

    /**
     * Init trace client configuration.
     */
    private void init() {
        tracer = new Tracer(traceConfig -> {
            traceConfig.setApp(CLIENT_APP);
            traceConfig.setRecordSource(CLIENT_SOURCE);
            traceConfig.setReportMode(systemDimensions.getOrDefault(DATATRACE_CLIENT_REPORT_MODE, DEFAULT_REPORT_MODE));
        });
        traceID = systemDimensions.get(TRACE_ID);
    }

    @Override
    public void sendStartTracking(StartupUtils.StartupPhase phase, long timestamp){
        logExecutor.execute(()-> tracer.processStart(traceID, phase.toString(), ""));
    }

    @Override
    public void sendEndTracking(
            StartupUtils.StartupPhase phase,
            boolean isSucceeded,
            Map<String, String> dimensions) {
        logExecutor.execute(() -> {
            Map<String, Object> metadata = new HashMap<>();
            if (MapUtils.isNotEmpty(dimensions)) {
                metadata.putAll(dimensions);
            }
            metadata.put(StatisticsUtils.PHASE, phase.toString());
            tracer.tags(traceID, phase.toString(), new HashMap<>(metadata));
            if (isSucceeded) {
                tracer.processSuccess(traceID, phase.toString());
            } else {
                tracer.processFail(traceID, phase.toString());
            }
        });
    }
}
