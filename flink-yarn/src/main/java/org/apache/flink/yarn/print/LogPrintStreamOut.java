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
