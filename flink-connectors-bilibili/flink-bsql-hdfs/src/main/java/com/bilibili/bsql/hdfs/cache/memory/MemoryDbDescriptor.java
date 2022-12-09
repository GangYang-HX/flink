package com.bilibili.bsql.hdfs.cache.memory;

import com.bilibili.bsql.common.TableInfo;
import com.bilibili.bsql.common.utils.FileUtil;
import com.bilibili.bsql.hdfs.cache.DbDescriptor;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

public class MemoryDbDescriptor implements DbDescriptor {
	private final static Logger LOG = LoggerFactory.getLogger(MemoryDbDescriptor.class);

	private final static String ORC = ".orc";
	private final static String LZO = ".lzo";

	private transient Cache<String, String> db;
	private int keyIndex;
	private String delimiter;
	private long memorySize;
	private String rootPath;

	private MemoryDbDescriptor() {
	}

	public void put(String key, String value) {
		db.put(key, value);
	}

	public void close() {
		db.cleanUp();
		FileUtils.deleteQuietly(new File(rootPath));
	}

	@Override
	public Iterator newIterator() {
		return null;
	}

	public byte[] get(String key) {
		String value = db.getIfPresent(key);
		if (value == null) {
			return null;
		}
		return value.getBytes();
	}

	@Override
	public boolean loadData2Db(String path, TableInfo tableInfo) throws Exception {
		return false;
	}

	public boolean loadData2Db(String localPath) throws Exception {
		boolean handleResult = true;

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
					long totalMemory = 0L;
					while ((line = br.readLine()) != null) {
						String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, delimiter);
						totalCount++;
						if (fields.length <= keyIndex) {
							LOG.warn("illegal line: {}, delimiter = {}, split line = {}", line, delimiter, Arrays.toString(fields));
							continue;
						}
						String key = fields[keyIndex];
						// 检查内存是否超过限制
						totalMemory += (key.getBytes().length + line.getBytes().length);
						if (memorySize < totalMemory) {
							LOG.warn("Memory exceeds data cache limit : memory size {}", totalMemory);
							handleResult = false;
							db.cleanUp();
							return handleResult;
						}
						db.put(key, line);
						successCount++;

					}
				}
				LOG.info("complete export file: {}, totalCount: {}", localFile.getAbsolutePath(), totalCount);

			}
			LOG.info("export localHdfsFile: {} to memoryDb success, cost: {} ms, total count:{}, success count: {} ", localPath, System.currentTimeMillis() - start, totalCount, successCount);
		} finally {
			if (handleResult) {
				FileUtil.delete(localPath, true);
			}
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


	public static class MemoryDbDescriptorBuilder {
		private long memorySize;
		private String delimiter;
		private int keyIndex;
		private String rootPath;

		public MemoryDbDescriptor build() {
			MemoryDbDescriptor memoryDbDescriptor = new MemoryDbDescriptor();
			memoryDbDescriptor.db = CacheBuilder.newBuilder()
				.maximumSize(this.memorySize)
				.build();
			memoryDbDescriptor.delimiter = this.delimiter;
			memoryDbDescriptor.keyIndex = this.keyIndex;
			memoryDbDescriptor.memorySize = this.memorySize;
			memoryDbDescriptor.rootPath = this.rootPath;
			return memoryDbDescriptor;
		}

		public static MemoryDbDescriptorBuilder newBuilder() {
			return new MemoryDbDescriptorBuilder();
		}

		public MemoryDbDescriptorBuilder withMemorySize(long size) {
			memorySize = size;
			return this;
		}

		public MemoryDbDescriptor.MemoryDbDescriptorBuilder withDelimiter(String delimiter) {
			this.delimiter = delimiter;
			return this;
		}

		public MemoryDbDescriptor.MemoryDbDescriptorBuilder whitKeyIndex(int keyIndex) {
			this.keyIndex = keyIndex;
			return this;
		}

		public MemoryDbDescriptor.MemoryDbDescriptorBuilder withRootPath(String rootPath) {
			this.rootPath = rootPath;
			return this;
		}

	}

}
