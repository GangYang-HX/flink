package org.apache.flink.yarn.print;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * This class extending form {@link LogBasedPrintStream} which supports the task manager outputs
 * error rolling log.
 */
public class LogPrintStreamErr extends LogBasedPrintStream {

    private static final Logger log = LoggerFactory.getLogger(LogPrintStreamErr.class);

    private static final PrintStream printStream = new LogPrintStreamErr();

    private LogPrintStreamErr() {
        super(System.err);
    }

    public static void redirectPrintStream() {
        System.setErr(printStream);
    }

    @Override
    protected void logPrint(String msg) {
        log.error(msg);
    }
}
