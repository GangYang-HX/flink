package org.apache.flink.metrics;

/** Enum for indicating the mode of metric group's query service. */
public enum QueryServiceMode {

    /* `INHERIT` is used to indicate the user doesn't set it. It will inherit the parent group */
    INHERIT(false),

    /* `DISABLED` is used to indicate the user disbale query service manually */
    DISABLED(true),

    /* `ENABLE` is used to indicate the user enable query service manually */
    ENABLE(false);

    private final boolean excluded;

    QueryServiceMode(boolean excluded) {
        this.excluded = excluded;
    }

    public boolean enabled() {
        return !excluded;
    }
}
