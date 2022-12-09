package org.apache.flink.contrib.streaming.state.diskUtils;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author humike
 */
public class DiskSelectStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackend.class);

	private static boolean init;

	private static File[] selectedFile;

	private static List<String> runShell(String shStr) throws InterruptedException, IOException {
		long start = System.nanoTime();
		List<String> strList = new ArrayList<>();
		ProcessBuilder builder = new ProcessBuilder("/bin/sh", "-c", shStr);
		builder.redirectErrorStream();
		try {
			final Process process = builder.start();

			//a thread deal with input buffer
			Thread t = new Thread(() -> {
				BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = null;

				try {
					while ((line = in.readLine()) != null) {
						strList.add(line);
					}
				} catch (IOException e) {
					LOG.error("io error happened during running disk select shell", e);
				} finally {
					try {
						in.close();
					} catch (IOException e) {
						LOG.error("error happened during close disk select shell", e);
					}
				}
			});
			t.start();
			t.join();

			process.waitFor();
		} catch (Exception e) {
			LOG.error("error happened during running disk select shell", e);
		}

		long cost = (System.nanoTime() - start) / 1000000L;
		LOG.info(String.format("the shell %s cost %s ms", shStr, cost));
		return strList;
	}

	private static File[] selectDisk(Map<File, Double> scores) {
		int diskNum = scores.size();
		if (diskNum <= 1) {
			return new File[]{scores.entrySet().iterator().next().getKey()};
		}
		double min = getMin(scores);

		Iterator<Map.Entry<File, Double>> iter = scores.entrySet().iterator();
		File[] files = new File[scores.size()];
		int n = 0;
		while (iter.hasNext()) {
			Map.Entry<File, Double> entry = iter.next();
			File file = entry.getKey();
			double score = entry.getValue();
			if (score > 5000) {
				continue;
			}
			if (score < 5) {
				files[n] = file;
				n++;
				continue;
			}
			if (score / min > 100) {
				continue;
			}
			files[n] = file;
			n++;
		}
		File[] selectedFiles = new File[n];
		System.arraycopy(files, 0, selectedFiles, 0, n);
		LOG.info("disk path selected" + Arrays.toString(selectedFiles));
		return selectedFiles;
	}

	private static double getMin(Map<File, Double> scores) {
		Double min = (double) 0;
		Iterator<Map.Entry<File, Double>> iter = scores.entrySet().iterator();
		if (iter.hasNext()) {
			min = iter.next().getValue();
		}
		while (iter.hasNext()) {
			Map.Entry<File, Double> entry = iter.next();
			Double tmp = entry.getValue();
			if (tmp < min) {
				min = tmp;
			}
		}
		return min;
	}

	private static double execScore(Double size, Double speed) {
		return size * speed;
	}

	private static List<String> execUsed() throws IOException, InterruptedException {
		String cmdProc = "du -sh /mnt/*/logs/yarn/nm/localdir/usercache/flink/appcache/application_*/container_*/state |sort -nk1";
		return runShell(cmdProc);
	}

	private static double execUse(List<String> br, String f) throws IndexOutOfBoundsException {
		double sumDisk = 0;
		for (String line : br) {
			String[] elements = line.split("\\s+");
			double diskSize = 0;
			if (Objects.equals(elements[1].split("/")[2], f)) {
				switch (elements[0].substring(elements[0].length() - 1)) {
					case "G":
						diskSize = Double.parseDouble(elements[0].substring(0, elements[0].length() - 1));
						break;
					case "M":
						diskSize = Double.parseDouble(elements[0].substring(0, elements[0].length() - 1)) / 1024;
						break;
					case "T":
						diskSize = Double.parseDouble(elements[0].substring(0, elements[0].length() - 1)) * 1024;
						break;
					default:
						break;
				}
				sumDisk += diskSize;
			}
		}
		return sumDisk;
	}

	private static List<String> buildMap() throws IOException, InterruptedException {
		String cmdProc = "df -hl |awk '{print $1,$6}'";
		return runShell(cmdProc);
	}

	private static String getMap(List<String> mp, String f) {
		String disk = null;
		for (String line : mp) {
			try {
				String tDisk = line.split("\\s+")[1].split("/")[2];
				if (Objects.equals(tDisk, f)) {
					disk = line.split("\\s+")[0].split("/")[2];
				}
			} catch (Exception ignored) {

			}
		}
		return disk;
	}

	private static double execSpeed(List<String> br, List<String> mp, String s) throws NullPointerException {
		double io = 0;
		int times = 0;
		String f = getMap(mp, s);
		if (f == null) {
			throw new IllegalArgumentException(String.format("disk used cannot map to disk io, %s, %s", s, mp));
		}
		for (String line : br) {
			String[] res = line.split("\\s+");
			if (res[0].equals(f) && times == 0) {
				times++;
			} else if (res[0].equals(f) && times > 0) {
				double diskUse = Double.parseDouble(res[res.length - 1]);
				io += diskUse;
				times++;
			}
		}
		return times == 0 ? 0 : io / times;
	}

	private static List<String> execSpeeds() throws InterruptedException, IOException {
		String cmdProc = "/usr/bin/iostat -x 1 2";
		return runShell(cmdProc);
	}

	private static void execDiskTest(Map<File, Double> dirs, List<String> dr, List<String> du, List<String> mp, File file) throws IndexOutOfBoundsException {
		String f = file.getAbsolutePath().split("/")[2];
		double size = execUse(du, f) + 0.001;
		double speed = execSpeed(dr, mp, f) + 0.001;
		double score = execScore(size, speed);
		dirs.put(file, score);
	}

	public static synchronized File[] init(File[] paths) throws IOException, InterruptedException, NullPointerException, IndexOutOfBoundsException {
		if (init) {
			return selectedFile;
		}
		HashMap<File, Double> dirs = new HashMap<>();
		List<String> dr = execSpeeds();
		List<String> du = execUsed();
		List<String> mp = buildMap();
		for (File f : paths) {
			execDiskTest(dirs, dr, du, mp, f);
		}
		selectedFile = selectDisk(dirs);
		if (selectedFile.length == 0) {
			throw new IllegalArgumentException("no disk been selected");
		}
		init = true;
		LOG.info("Better Disk Selector running successed");
		return selectedFile;
	}
}
