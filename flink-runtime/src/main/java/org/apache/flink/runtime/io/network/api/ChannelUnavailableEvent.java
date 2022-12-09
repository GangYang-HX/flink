package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

public class ChannelUnavailableEvent extends RuntimeEvent {

    private final int channelIndex;

    public ChannelUnavailableEvent(int channelIndex) {
        this.channelIndex = channelIndex;
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    //
    //  These methods are inherited form the generic serialization of AbstractEvent
    //  but would require the ChannelUnavailableEvent to be mutable. Since all serialization
    //  for events goes through the EventSerializer class, which has special serialization
    //  for the ChannelUnavailableEvent, we don't need these methods
    //

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    public int getChannelIndex() {
        return channelIndex;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(channelIndex);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != ChannelUnavailableEvent.class) {
            return false;
        } else {
            ChannelUnavailableEvent that = (ChannelUnavailableEvent) other;
            return that.channelIndex == this.channelIndex;
        }
    }

    @Override
    public String toString() {
        return String.format("ChannelUnavailableEvent for channel %d", channelIndex);
    }
}
