package com.bilibili.bsql.common.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jackt
 * @date 19-3-29
 */
public final class FileUtil {
	private FileUtil() {
	}

	public static List<File> readFile(String fileDir) {
		List<File> fileList = new ArrayList<>();
		File file = new File(fileDir);
		File[] files = file.listFiles();
		if (files == null) {
			return new ArrayList<>();
		}
		for (File f : files) {
			if (f.isFile()) {
				fileList.add(f);
			} else if (f.isDirectory()) {
				System.out.println(f.getAbsolutePath());
				readFile(f.getAbsolutePath());
			}
		}
		return fileList;
	}

	/**
	 * 判断文件是否写完, 没写完等待一秒
	 *
	 * @param file
	 * @return 写完返回 false
	 * @throws Exception
	 */
	public static boolean checkFileWritingOn(File file) throws InterruptedException {
		long newLen, oldLen = 0;
		while (true) {
			newLen = file.length();
			if ((newLen - oldLen) > 0) {
				oldLen = newLen;
				Thread.sleep(1_000L);
			} else {
				return false;
			}
		}
	}

	/**
	 * 删除文件或者文件夹
	 *
	 * @param path
	 * @param cyclic 是否循环删除
	 */
	public static void delete(String path, boolean cyclic) {
		File file = new File(path);
		if (!file.exists()) {
			return;
		}
		if (!cyclic) {
			file.delete();
		}
		for (File subFile : file.listFiles()) {
			if (subFile.isDirectory()) {
				delete(subFile.getAbsolutePath(), cyclic);
			} else {
				subFile.delete();
			}
		}
		file.delete();
	}

}
