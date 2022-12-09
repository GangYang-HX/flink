package com.bilibili.bsql.hdfs.internal.selector;

import com.alibaba.fastjson.JSON;

import com.bilibili.bsql.common.utils.DateFilePatternUtils;
import com.bilibili.bsql.common.utils.HdfsFileSystem;
import com.bilibili.bsql.hdfs.internal.FilePartition;
import com.bilibili.bsql.hdfs.internal.FileSourceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zhangyang
 * @Date:2020/5/25
 * @Time:4:52 PM
 */
public class DateBasedSelector implements FileSelector {

    private final static Logger LOG = LoggerFactory.getLogger(DateBasedSelector.class);

    @Override
    public List<FilePartition> selectAndSort(int index, int num, FileSourceProperties properties) throws Exception {
        String path = properties.getPath();
        LinkedHashMap<String, String> datePath = DateFilePatternUtils.datePatternConvert(path);
        HdfsFileSystem fs = new HdfsFileSystem.Builder().build(properties.getConfDir(), properties.getUserName());
        LinkedHashMap<String, List<String>> dateSortedFiles = new LinkedHashMap<>();
        for (String key : datePath.keySet()) {
            try {
                List<String> fileList = fs.listFileStatus(datePath.get(key), 2).stream().map(it -> it.getPath().toString()).collect(Collectors.toList());
                Collections.sort(fileList);
                dateSortedFiles.put(key, fileList);
            } catch (FileNotFoundException e) {
                continue;
            }
        }
        fs.close();
        return select(index, num, dateSortedFiles);
    }

    private List<FilePartition> select(int index, int num, LinkedHashMap<String, List<String>> dateSortedFiles) {
        List<String> subFile = new ArrayList<>();
        for (String date : dateSortedFiles.keySet()) {
            List<String> files = dateSortedFiles.get(date);
            for (int i = 0; i < files.size(); i++) {
                if (i % num == index) {
                    subFile.add(files.get(i));
                }
            }
        }
        LOG.info("index:{},num:{},allocated:{}", index, num, JSON.toJSONString(subFile));
        FilePartition partition = new FilePartition(subFile);
        return Collections.singletonList(partition);
    }
}
