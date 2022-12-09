package com.bilibili.bsql.hdfs.cache.memory;

import com.alibaba.fastjson.JSONArray;
import com.bilibili.bsql.common.TableInfo;
import com.bilibili.bsql.common.utils.FileUtil;
import com.bilibili.bsql.hdfs.cache.AbstractHiveSideDbDescriptor;
import com.bilibili.bsql.hdfs.cache.DbDescriptor;
import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class HiveSideMemoryDbDescriptor extends AbstractHiveSideDbDescriptor implements DbDescriptor {
    private final static Logger LOG = LoggerFactory.getLogger(HiveSideMemoryDbDescriptor.class);

    private final static String ORC = ".orc";
    private final static String LZO = ".lzo";

    private transient Cache<String, String> db;
    private int keyIndex;
    private String delimiter;
    private String rootPath;
    private Configuration conf;
    private String uuId;

    private HiveSideMemoryDbDescriptor() {
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


    public boolean loadData2Db(String path, TableInfo tableInfo) throws Exception {
        String format = ((HiveSideTableInfo) tableInfo).getFormat();
        switch (format) {
            case HiveSideTableInfo.TABLE_FORMAT_TEXT:
                return loadData2Db(path);
            case HiveSideTableInfo.TABLE_FORMAT_ORC:
                return loadOrcData2Db(path);
            default:
                throw new Exception("Currently unsupported table types:" + format);
        }
    }


    public boolean loadOrcData2Db(String localPath) throws Exception {
        try {
            File[] localFiles = new File(localPath).listFiles();
            LOG.info("start export localPath : {} data to memoryDb", localPath);
            long lineCount = 0;
            long totalMemory = 0L;
            long start = System.currentTimeMillis();
            for (File localFile : localFiles) {
                if (localFile.isHidden()) {
                    continue;
                }
                long fileLineCount = 0;
                String filePath = localFile.getPath();
                filePath = "file://".concat(filePath);
                LOG.info("start to load filePath : {} ", filePath);
                Path path = new Path(filePath);
                Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
                RecordReader rows = reader.rows();
                TypeDescription schema = reader.getSchema();
                VectorizedRowBatch batch = schema.createRowBatch();
                while (rows.nextBatch(batch)) {
                    for (int r = 0; r < batch.size; r++) {
                        String keyValue = "";
                        List<Object> line = new ArrayList<>();
                        List<TypeDescription> fieldTypes = schema.getChildren();
                        for (int c = 0; c < batch.cols.length; c++) {
                            ColumnVector col = batch.cols[c];
                            TypeDescription typeDescription = fieldTypes.get(c);
                            Object convertValue = convert(col, typeDescription, r);
                            if (c == keyIndex) {
                                keyValue = String.valueOf(convertValue);
                            }
                            line.add(convertValue);
                        }
                        String jsonString = JSONArray.toJSONString(line);
                        db.put(keyValue, jsonString);
                        totalMemory += (keyValue.getBytes().length + jsonString.getBytes().length);
                        lineCount++;
                        fileLineCount++;
                    }
                }
                LOG.info("complete export file: {}, fileLineCount: {}", localFile.getAbsolutePath(), fileLineCount);
            }
            if (lineCount == 0) {
                LOG.warn("current localHdfsFile:{} does not have any file", localPath);
                return false;
            } else {
                LOG.info("export localHdfsFile: {} to memoryDb success, cost: {} ms, lineCount(未按joinKey去重):{}, totalMemory: {} bytes.", localPath, System.currentTimeMillis() - start, lineCount, totalMemory);
            }
        } catch (Exception e) {
            LOG.error("遍历数据异常,msg:{}", e.getMessage(), e);
            throw e;
        } finally {
            FileUtil.delete(localPath, true);
            LOG.info("Local hdfs file = {} has been deleted. ", localPath);
        }
        return true;
    }


    public boolean loadData2Db(String localPath) throws Exception {
        try {
            long lineCount = 0;
            long totalMemory = 0L;
            long start = System.currentTimeMillis();
            LOG.info("start export localHdfsFile: {} to memoryDb", localPath);
            File[] localFiles = new File(localPath).listFiles();
            for (File localFile : localFiles) {
                if (localFile.isHidden()) {
                    continue;
                }
                long fileLineCount = 0;
                try (InputStream inputStream = wrapInputStream(new FileInputStream(localFile), localFile.getName());
                     BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, delimiter);
                        if (fields.length <= keyIndex) {
                            LOG.warn("illegal line: {}, delimiter = {}, split line = {}", line, delimiter, Arrays.toString(fields));
                            continue;
                        }
                        String key = fields[keyIndex];
                        db.put(key, line);
                        totalMemory += (key.getBytes().length + line.getBytes().length);
                        lineCount++;
                        fileLineCount++;

                    }
                }
                LOG.info("complete export file: {}, fileLineCount: {}", localFile.getAbsolutePath(), fileLineCount);
            }
            if (lineCount == 0) {
                LOG.warn("current localHdfsFile:{} does not have any file", localPath);
                return false;
            } else {
                LOG.info("export localHdfsFile: {} to memoryDb success, cost: {} ms, lineCount(未按joinKey去重):{}, totalMemory: {} bytes.", localPath, System.currentTimeMillis() - start, lineCount, totalMemory);
            }

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
        return origin;
    }


    public static class HiveSideMemoryDbDescriptorBuilder {
        private String delimiter;
        private int keyIndex;
        private String rootPath;
        private String uuId;

        public HiveSideMemoryDbDescriptor build() {
            HiveSideMemoryDbDescriptor memoryDbDescriptor = new HiveSideMemoryDbDescriptor();
            memoryDbDescriptor.db = CacheBuilder.newBuilder().build();
            memoryDbDescriptor.delimiter = this.delimiter;
            memoryDbDescriptor.keyIndex = this.keyIndex;
            memoryDbDescriptor.rootPath = this.rootPath;
            memoryDbDescriptor.conf = new Configuration();
            memoryDbDescriptor.uuId = this.uuId;
            return memoryDbDescriptor;
        }

        public static HiveSideMemoryDbDescriptorBuilder newBuilder() {
            return new HiveSideMemoryDbDescriptorBuilder();
        }


        public HiveSideMemoryDbDescriptor.HiveSideMemoryDbDescriptorBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public HiveSideMemoryDbDescriptor.HiveSideMemoryDbDescriptorBuilder withKeyIndex(int keyIndex) {
            this.keyIndex = keyIndex;
            return this;
        }

        public HiveSideMemoryDbDescriptor.HiveSideMemoryDbDescriptorBuilder withRootPath(String rootPath) {
            this.rootPath = rootPath;
            return this;
        }

        public HiveSideMemoryDbDescriptor.HiveSideMemoryDbDescriptorBuilder withTaskUuid(String uuid) {
            this.uuId = uuid;
            return this;
        }

    }

}
