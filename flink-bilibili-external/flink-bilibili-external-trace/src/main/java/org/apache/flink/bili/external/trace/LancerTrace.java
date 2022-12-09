package org.apache.flink.bili.external.trace;

import com.bilibili.lancer.trace.client.TraceClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.trace.Trace;


@Slf4j
public class LancerTrace implements Trace {
    private static TraceClient traceClient;
    private final String traceId;

    static {
        String traceClientName = "lancer2-collector";
        traceClient = new TraceClient(traceClientName, "http://lancer-router.bilibili.co/lancer/trace2");
        traceClient.start();
    }

    public LancerTrace(String traceId) {
        this.traceId = traceId;
    }



    @Override
    public void traceEvent(long time, long value, String key, String url, int delay, String sinkType) {
        traceClient.traceEventWithDest(time, value, key, traceId, url, delay, sinkType, "lancer2-bsql", "default");
    }

    @Override
    public void traceError(long time, long value, String key,String url, String error, String sinkType) {
        traceClient.traceErrorWithDest(time, value, key, traceId, url, error, sinkType, "lancer2-bsql", "default");
    }
}
