package com.bilibili.bsql.common.metrics;

/**
 * A common interface used to metric sink or side table.
 */
public interface Metrics {

    /**
     * WindowSize controls the number of values that contribute to the reported statistics.
     * For example, if windowSize is set to 3 and the values {1,2,3,4,5} have been added in
     * that order then the available values are {3,4,5} and all reported statistics will
     * be based on these values.
     */
    int WINDOW_SIZE = 10_000;

}
