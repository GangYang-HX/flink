/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.bili.writer.trigger;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.bili.writer.extractor.PartitionTimeExtractor;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.spliter.PathSplitter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.bili.writer.FileSystemOptions.*;


public class ProcTimePartitionCommitTigger extends PartitionCommitTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(ProcTimePartitionCommitTigger.class);

    private final ProcessingTimeService procTimeService;

    private final PartitionTimeExtractor extractor;
    private final PathSplitter splitter;
    private final boolean pathContainPartitionKey;

    public ProcTimePartitionCommitTigger(
        boolean isRestored,
        OperatorStateStore stateStore,
        Configuration conf,
        ClassLoader cl,
        ProcessingTimeService procTimeService,
        List<String> partitionKeys, TableMetaStoreFactory metaStoreFactory) throws Exception {
        super(isRestored, stateStore, conf);
        this.procTimeService = procTimeService;
        this.partitionKeys = partitionKeys;
        this.commitDelay = conf.getLong(SINK_PARTITION_COMMIT_DELAY);

        this.extractor = PartitionTimeExtractor.create(
            cl,
            conf.getString(PARTITION_TIME_EXTRACTOR_KIND),
            conf.getString(PARTITION_TIME_EXTRACTOR_CLASS),
            conf.getString(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN));

        this.splitter = PathSplitter.create(
            cl,
            conf.getString(PARTITION_SPLIT_KIND),
            conf.getString(PARTITION_SPLIT_CLASS));
        this.metaStoreFactory = metaStoreFactory;
        this.pathContainPartitionKey = conf.getBoolean(SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY);
    }

    @Override
    public List<String> committablePartitions(long checkpointId) throws IOException {
        List<String> needCommit = new ArrayList<>();
        long currentProcTime = procTimeService.getCurrentProcessingTime();
        this.lastTimestamp = currentProcTime;
        Iterator<String> iter = pendingPartitions.iterator();
        while (iter.hasNext()) {
            String partition = iter.next();
            Long partTime = extractor.extract(
                partitionKeys, splitter.splitPath(pathContainPartitionKey, partitionKeys, new Path(partition)));
            LOG.info("judging time parition {}, wk {}, parttime {}",
                partition, new Timestamp(currentProcTime), new Timestamp(partTime));
            if (currentProcTime > partTime + commitDelay) {
                needCommit.add(partition);
            }
        }
        return needCommit;
    }

    @Override
    public void checkAndAddEmptyPartitions(long checkpointId) throws Exception {
        super.checkAndAddEmptyPartitions(checkpointId);
    }
}
