package org.apache.flink.yarn.print;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * This class extending form {@link LogBasedPrintStream} which supports the task manager outputs
 * info rolling log.
 */
public class LogPrintStreamOut extends LogBasedPrintStream {

    private static final Logger log = LoggerFactory.getLogger(LogPrintStreamOut.class);

    private static final PrintStream printStream = new LogPrintStreamOut();

    private LogPrintStreamOut() {
        super(System.out);
    }

    public static void redirectPrintStream() {
        System.setOut(printStream);
    }

    @Override
    protected void logPrint(String msg) {
        log.info(msg);
    }
}
