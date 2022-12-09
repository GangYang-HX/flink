package com.bilibili.bsql.hdfs.cache.rocksdb;


import com.bilibili.bsql.common.TableInfo;
import com.bilibili.bsql.common.utils.FileUtil;
import com.bilibili.bsql.hdfs.cache.DbDescriptor;
import com.bilibili.bsql.hdfs.cache.HdfsTableCache;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.hadoop.conf.Configuration;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RocksDbDescriptor implements DbDescriptor {


	private final static Logger LOG = LoggerFactory.getLogger(RocksDbDescriptor.class);
	private String rootPath;
	private RocksDB db;
	private String delimiter;
	private int keyIndex;
	private final static String ROCKS_DB = "rocksdb";
	private final static String ORC = ".orc";
	private final static String LZO = ".lzo";

	/**
	 * The number of (re)tries for loading the RocksDB JNI library.
	 */
	private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

	/**
	 * Flag whether the native library has been loaded.
	 */
	private static boolean rocksDbInitialized = false;

	public static void init() {
		// 加载jni
		try {
			if (!rocksDbInitialized) {
				String localDirs = System.getProperty("java.io.tmpdir");
				if (StringUtils.isNotBlank(localDirs)) {
					ensureRocksDBIsLoaded(localDirs);
					LOG.info("rockDb jni root tmp path: {}", localDirs);
				} else {
					throw new RuntimeException("the environment variable java.io.tmpdir directory is empty.");
				}
			}
		} catch (Exception e) {
			LOG.info("hdfs side loading lib failure", e);

			throw new RuntimeException(e);
		}
	}

	private RocksDbDescriptor() {

	}

	public void put(String key, String value) throws Exception {
		db.put(key.getBytes(), value.getBytes());
	}

	public byte[] get(String key) throws Exception {
		return db.get(key.getBytes());
	}

	public void close() {
		db.close();
		FileUtils.deleteQuietly(new File(rootPath));
		LOG.info("rocksdb delete resource root: {}", rootPath);
	}

	@Override
	public Iterator newIterator() {
		return null;
	}


	@Override
	public boolean loadData2Db(String path, TableInfo tableInfo) throws Exception {
		return false;
	}

	public boolean loadData2Db(String localPath) throws Exception {

		try {
			long start = System.currentTimeMillis();
			int totalCount = 0, successCount = 0;
			LOG.info("start export localHdfsFile: {} to rocksdb", localPath);
			File[] localFiles = new File(localPath).listFiles();
			for (File localFile : localFiles) {
				if (localFile.isHidden()) {
					continue;
				}
				try (InputStream inputStream = wrapInputStream(new FileInputStream(localFile), localFile.getName());
					 BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
					String line;
					while ((line = br.readLine()) != null) {
						String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, delimiter);
						totalCount++;
						if (fields.length <= keyIndex) {
							LOG.warn("illegal line: {}, delimiter = {}, split line = {}", line, delimiter, Arrays.toString(fields));
							continue;
						}
						String key = fields[keyIndex];
						db.put(key.getBytes(), line.getBytes());
						successCount++;
					}
				}
				LOG.info("complete export file: {}, totalCount: {}", localFile.getAbsolutePath(), totalCount);
			}
			LOG.info("export localHdfsFile: {} to rocksdb success, cost: {} ms, total count:{}, success count: {} ", localPath, System.currentTimeMillis() - start, totalCount, successCount);
		} finally {
			FileUtil.delete(localPath, true);
			LOG.info("Local hdfs file = {} has been deleted. ", localPath);
		}
		return true;
	}

	private InputStream wrapInputStream(InputStream origin, String fileName) throws IOException {
		if (StringUtils.endsWithIgnoreCase(fileName, LZO)) {
			LzopCodec codec = new LzopCodec();
			codec.setConf(new Configuration());
			return codec.createInputStream(origin);
		}
		if (StringUtils.endsWithIgnoreCase(fileName, ORC)) {
			// todo
			return origin;
		}
		return origin;
	}


	static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
		synchronized (HdfsTableCache.class) {
			if (!rocksDbInitialized) {

				final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
				LOG.info("Attempting to load RocksDB native library and store it under '{}'", tempDirParent);

				Throwable lastException = null;
				final List<String> rocksLibPaths = new ArrayList<String>();
				for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
					File rocksLibFolder = null;
					try {
						// when multiple instances of this class and RocksDB exist in different
						// class loaders, then we can see the following exception:
						// "java.lang.UnsatisfiedLinkError: Native Library /path/to/temp/dir/librocksdbjni-linux64.so
						// already loaded in another class loader"

						// to avoid that, we need to add a random element to the library file path
						// (I know, seems like an unnecessary hack, since the JVM obviously can handle multiple
						//  instances of the same JNI library being loaded in different class loaders, but
						//  apparently not when coming from the same file path, so there we go)

						rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());
						rocksLibPaths.add(rocksLibFolder.getPath());
						// make sure the temp path exists
						LOG.debug("Attempting to create RocksDB native library folder {}", rocksLibFolder);
						// noinspection ResultOfMethodCallIgnored
						rocksLibFolder.mkdirs();

						// explicitly load the JNI dependency if it has not been loaded before
						NativeLibraryLoader.getInstance().loadLibrary(rocksLibFolder.getAbsolutePath());

						// this initialization here should validate that the loading succeeded
						RocksDB.loadLibrary();

						// seems to have worked
						LOG.info("Successfully loaded RocksDB native library");
						rocksDbInitialized = true;
						ShutdownHookUtil.addShutdownHook(
							new AutoCloseable(){
								@Override
								public void close() throws Exception {
									for (String path : rocksLibPaths) {
										FileUtils.deleteQuietly(new File(path));
									}
								}
							},
							HdfsTableCache.class.getSimpleName(),
							LOG
						);
						return;
					} catch (Throwable t) {
						lastException = t;
						LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);

						// try to force RocksDB to attempt reloading the library
						try {
							resetRocksDBLoadedFlag();
						} catch (Throwable tt) {
							LOG.debug("Failed to reset 'initialized' flag in RocksDB native code loader", tt);
						}

						org.apache.flink.util.FileUtils.deleteDirectoryQuietly(rocksLibFolder);
					}
				}

				throw new IOException("Could not load the native RocksDB library", lastException);
			}
		}

	}

	@VisibleForTesting
	static void resetRocksDBLoadedFlag() throws Exception {
		final Field initField = org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
		initField.setAccessible(true);
		initField.setBoolean(null, false);
	}


	public static class RocksDbDescriptorBuilder {
		private String rootPath;
		private String taskUuid;
		private String delimiter;
		private int keyIndex;

		public RocksDbDescriptor build() {
			RocksDbDescriptor rocksDbDescriptor = new RocksDbDescriptor();
			try {
				init();
				String rocksDbPath = rootPath + "/" + taskUuid + "/" + RocksDbDescriptor.ROCKS_DB;
				rocksDbDescriptor.rootPath = rootPath + "/" + taskUuid;
				rocksDbDescriptor.delimiter = this.delimiter;
				rocksDbDescriptor.keyIndex = keyIndex;
				Options op = new Options();
				op.setCreateIfMissing(true);
				rocksDbDescriptor.db = RocksDB.open(op, rocksDbPath);

			} catch (RocksDBException e) {
				e.printStackTrace();
			}
			return rocksDbDescriptor;
		}

		public static RocksDbDescriptorBuilder newBuilder() {
			return new RocksDbDescriptorBuilder();
		}

		public RocksDbDescriptorBuilder withTaskUuid(String taskUuid) {
			this.taskUuid = taskUuid;
			return this;
		}

		public RocksDbDescriptorBuilder withRootPath(String rootPath) {
			this.rootPath = rootPath;
			return this;
		}

		public RocksDbDescriptorBuilder withDelimiter(String delimiter) {
			this.delimiter = delimiter;
			return this;
		}

		public RocksDbDescriptorBuilder whitKeyIndex(int keyIndex) {
			this.keyIndex = keyIndex;
			return this;
		}

	}

}
