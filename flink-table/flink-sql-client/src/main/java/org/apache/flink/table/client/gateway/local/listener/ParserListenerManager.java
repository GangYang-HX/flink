package org.apache.flink.table.client.gateway.local.listener;

import java.util.ArrayList;
import java.util.List;

/** listener manager. */
public class ParserListenerManager {
    List<ParserListener> listeners = new ArrayList<>();

    public ParserListenerManager() {}

    public void addListener(ParserListener parserListener) {
        this.listeners.add(parserListener);
    }

    public void removeListener(ParserListener parserListener) {
        this.listeners.remove(parserListener);
    }

    public void process(Object... parsed) {
        for (ParserListener listener : this.listeners) {
            listener.onParsed(parsed);
        }
    }
}
