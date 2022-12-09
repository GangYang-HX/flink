package org.apache.flink.yarn.print;

import java.io.PrintStream;

/**
 * @author : luotianran
 * @version V1.0
 * @Description:
 * @date Date : 2022年03月30日
 */
public abstract class LogBasedPrintStream extends PrintStream {

	protected LogBasedPrintStream(PrintStream printStream) {
		super(printStream);
	}

	@Override
	public void print(boolean b) {
		logPrint(b ? "true" : "false");
	}

	@Override
	public void print(char c) {
		logPrint(String.valueOf(c));
	}

	@Override
	public void print(char[] s) {
		logPrint(s == null ? null : new String(s));
	}

	@Override
	public void print(double d) {
		logPrint(String.valueOf(d));
	}

	@Override
	public void print(float f) {
		logPrint(String.valueOf(f));
	}

	@Override
	public void print(int i) {
		logPrint(String.valueOf(i));
	}

	@Override
	public void print(long l) {
		logPrint(String.valueOf(l));
	}

	@Override
	public void print(Object obj) {
		logPrint(String.valueOf(obj));
	}

	@Override
	public void print(String s) {
		if (s == null) {
			s = "null";
		}
		logPrint(s);
	}

	@Override
	public void println(boolean x) {
		print(x);
	}

	@Override
	public void println(char x) {
		print(x);
	}

	@Override
	public void println(char x[]) {
		print(x);
	}

	@Override
	public void println(double x) {
		print(x);
	}

	@Override
	public void println(float x) {
		print(x);
	}

	@Override
	public void println(long x) {
		print(x);
	}

	@Override
	public void println(int x) {
		print(x);
	}

	@Override
	public void println(Object x) {
		print(x);
	}

	@Override
	public void println(String x) {
		print(x);
	}

	protected abstract void logPrint(String msg);

}
