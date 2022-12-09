package org.apache.flink.yarn.print;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * @author : luotianran
 * @version V1.0
 * @Description:
 * @date Date : 2022年03月30日
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
