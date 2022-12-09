package org.apache.flink.table.client.gateway.local.listener;

/** parse listener interface. */
public interface ParserListener {
    void onParsed(Object... parsed);
}
