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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.table.ContinuousPartitionFetcher;
import org.apache.flink.connector.file.table.PartitionFetcher;
import org.apache.flink.connector.file.table.PartitionFetcher.Context.ComparablePartitionValue;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** A {@link ContinuousPartitionFetcher} for hive table. */
@Internal
public class HiveContinuousPartitionFetcher<T extends Comparable<T>>
        implements ContinuousPartitionFetcher<Partition, T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HiveContinuousPartitionFetcher.class);

    private final SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
    private final SimpleDateFormat hourFormat = new SimpleDateFormat("yyyyMMddHH");

    @Override
    @SuppressWarnings("unchecked")
    public List<Tuple2<Partition, T>> fetchPartitions(
            Context<Partition, T> context, T previousOffset) throws Exception {
        List<Tuple2<Partition, T>> partitions = new ArrayList<>();
        List<ComparablePartitionValue> partitionValueList =
                context.getComparablePartitionValueList(); //TODO 如果按两级分区进行相关逻辑处理，这里就可以避免每次获取所有分区然后进行过滤
        for (ComparablePartitionValue<List<String>, T> partitionValue : partitionValueList) {
            T partitionOffset = partitionValue.getComparator();
            if (partitionOffset.compareTo(previousOffset) >= 0) {
                Partition partition =
                        context.getPartition(partitionValue.getPartitionValue())
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        String.format(
                                                                "Fetch partition fail for hive "
                                                                        + "table %s.",
                                                                context.getTablePath())));
                partitions.add(new Tuple2<>(partition, partitionValue.getComparator()));
            }
        }
//        createVirtualPartition(partitions, context, previousOffset);
        return removePreCreatePartition(partitions, context);
    }

    /**
     * 现在的分区过滤逻辑是获取大于start_offset的所有分区，但数仓有一种建表模式会导致直接创建今天23点的分区，那么就会导致这个分区真正有数据的时候不会被读取
     *
     * @param partitions
     */
    private List<Tuple2<Partition, T>> removePreCreatePartition(
            List<Tuple2<Partition, T>> partitions,
            Context<Partition, T> context) {
        Preconditions.checkArgument(
                checkValidPartition(context),
                "The current task is not a valid partition");
        int partitionSize = context.getPartitionKey().size();
        final List<String> parValues = getVirPartValues(partitionSize, 0);
        Predicate<Tuple2<Partition, T>> partitionPredicate = new Predicate<Tuple2<Partition, T>>() {
            @Override
            public boolean test(Tuple2<Partition, T> tuple2partition) {
                Long currentPartition = Long.valueOf(Joiner.on("")
                        .join(tuple2partition.f0.getValues()));
                Long latestPartition = Long.valueOf(Joiner.on("").join(parValues));
                return currentPartition.compareTo(latestPartition) <= 0;
            }
        };
        return partitions.stream().filter(partitionPredicate).collect(Collectors.toList());
    }


    private boolean checkValidPartition(Context<Partition, T> context) {
        if (context.getPartitionKey().size() < 1) {
            LOG.info(
                    "Non-partitioned table, no need to build virtual partitions，tableInfo={}",
                    context.getTablePath());
            return false;
        }
        List<FieldSchema> fieldSchemas = (List<FieldSchema>) context.getPartitionKey();
        if (fieldSchemas.size() > 2) { //只支持二级分区
            LOG.info(
                    "Create virtual partitions, only support secondary partitions，tableInfo={}",
                    context.getTablePath());
            return false;
        }
        final List<String> supportKey = Arrays.asList("log_date", "log_hour");
        List<String> partitionKeys = fieldSchemas.stream().map(item -> item.getName()).collect(
                Collectors.toList());
        if (partitionKeys.stream().filter(name -> !supportKey.contains(name)).count() > 0L) {
            LOG.info(
                    "Create a virtual partition, the partition only supports (log_date, log_hour),tableInfo={},partitionKeys={}",
                    context.getTablePath(),
                    partitionKeys);
            return false;
        }
        return true;
    }

    private void createVirtualPartition(
            List<Tuple2<Partition, T>> partitions,
            Context<Partition, T> context,
            T previousOffset) {
        try {
            LOG.info("Start creating hive virtual partitions");
            Preconditions.checkArgument(
                    checkValidPartition(context),
                    "The current task is not a valid partition");
            int partitionSize = context.getPartitionKey().size();
            List<String> partValueStrs = partitions
                    .stream()
                    .map(new Function<Tuple2<Partition, T>, String>() {
                        @Override
                        public String apply(Tuple2<Partition, T> partitionTTuple2) {
                            Partition partition = partitionTTuple2.f0;
                            return Joiner.on("").join(partition.getValues());
                        }
                    }).collect(Collectors.toList());
            List<Partition> virPartitions = new ArrayList<>(); //保存从当前时间往前推，看看有多少个分区未创建
            int backNum = 0;
            Partition partition = partitions.get(partitions.size() - 1).f0; //以最新的partition为准
            boolean pathContainPartitionKey = partition
                    .getSd()
                    .getLocation()
                    .contains("/log_date="); //TODO 后续可以考虑使用正则
            List<String> parValues = getVirPartValues(partitionSize, backNum);
            while (checkStartOffset(parValues, previousOffset)) {
                if (context.getPartition(parValues).isPresent()) {
                    LOG.info(
                            "The currently created virtual partition already exists,{}",
                            parValues);
                    if (partValueStrs.contains(Joiner.on("").join(parValues))) {
                        break;
                    }
                    LOG.info(
                            "When creating a virtual partition, a new partition information is added to the hive meta,{}",
                            parValues);
                    return;
                }
                Map<String, String> parameters = new HashMap<>(partition.getParameters());
                parameters.put("hive_virtual_partition", "hive_virtual_partition"); //用于标识当前分区是虚拟分区
                Partition virPartition = new Partition(
                        parValues,
                        context.getTablePath().getDatabaseName(),
                        context.getTablePath().getObjectName(),
                        createTimeOf(parValues),
                        0,
                        createStorageDescriptor(
                                partition,
                                parValues,
                                (List<FieldSchema>) context.getPartitionKey(),
                                pathContainPartitionKey),
                        parameters
                );
                virPartitions.add(virPartition);
                parValues = getVirPartValues(partitionSize, ++backNum);
            }
//            //如果在start_offset之后存在多个应创建未创建的分区，优先取第一个分区进行处理
//            Partition minVirPartition = virPartitions.stream()
//                    .min((o1, o2) -> o1.getCreateTime() - o2.getCreateTime()).get();
//            partitions.add(Tuple2.of(minVirPartition, null));

            for (Partition virPart : virPartitions) {
                partitions.add(Tuple2.of(
                        virPart,
                        (T) Integer.valueOf(createTimeOf(virPart.getValues()))));
            }
            LOG.info(
                    "Virtual partition created successfully,vir partition location : {}",
                    virPartitions
                            .stream()
                            .map(item -> item.getSd().getLocation())
                            .collect(Collectors.toList()));
        } catch (Exception e) {
            LOG.error("Virtual partition creation failed,{}", ExceptionUtils.getStackTrace(e));
        }
    }

    private boolean checkStartOffset(List<String> parValues, T previousOffset) throws Exception {
        String dayModel = "log_date=%s";
        String hourModel = "log_date=%s/log_hour=%s";
        Long partitionTime = dayFormat.parse(parValues.get(0)).getTime();
        String partitionName = String.format(dayModel, parValues.get(0));
        if (parValues.size() > 1) {
            partitionTime = hourFormat.parse(parValues.get(0) + parValues.get(1)).getTime();
            partitionName = String.format(hourModel, parValues.get(0), parValues.get(1));
        }
        LOG.info(
                "Determine whether the current partition is after start_offset，parValues={},previousOffset={},partitionTime={},partitionName={}",
                parValues,
                String.valueOf(previousOffset),
                partitionTime,
                partitionName);
        if (previousOffset instanceof Long) {
            return partitionTime.compareTo((Long) previousOffset) >= 0;
        }
        return partitionName.compareTo((String) previousOffset) >= 0;
    }

    private int createTimeOf(List<String> parValues) throws Exception {
        if (parValues.size() == 1) {
            return Integer.valueOf(String.valueOf(
                    dayFormat.parse(parValues.get(0)).getTime() / 1000));
        }
        return Integer.valueOf(String.valueOf(
                hourFormat.parse(parValues.get(0) + parValues.get(1)).getTime() / 1000));
    }

    /**
     * 获取分区信息
     *
     * @param partitionSize 分区层级
     * @param backNum 用于表示回退到上几个分区
     *
     * @return
     */
    private List<String> getVirPartValues(int partitionSize, int backNum) {
        Calendar calendar = Calendar.getInstance();
        String log_date = dayFormat.format(calendar.getTime().getTime());
        List<String> parValus = Arrays.asList(log_date);
        if (partitionSize > 1) {
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            String log_hour = (hour < 10 ? "0" + hour : String.valueOf(hour));
            parValus = Arrays.asList(log_date, log_hour);
        }
        if (parValus.size() != partitionSize) {
            return Collections.emptyList();
        }
        if (backNum == 0) { //不需要回退
            return parValus;
        }
        if (parValus.size() == 1) {
            calendar.add(Calendar.DAY_OF_YEAR, -backNum);
            parValus = Arrays.asList(dayFormat.format(calendar.getTime().getTime()));
        } else {
            calendar.add(Calendar.HOUR_OF_DAY, -backNum);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            String log_hour = (hour < 10 ? "0" + hour : String.valueOf(hour));
            parValus = Arrays.asList(dayFormat.format(calendar.getTime().getTime()), log_hour);
        }
        return parValus;
    }


    private StorageDescriptor createStorageDescriptor(
            Partition partition,
            List<String> partValues,
            List<FieldSchema> partitionKeys, boolean pathContainPartitionKey) {
        StorageDescriptor virSd = new StorageDescriptor(partition.getSd());
        Path currPath = new Path(partition.getSd().getLocation());
        Path tablePath = currPath;
        int level = partitionKeys.size();
        while (level-- > 0) {
            tablePath = tablePath.getParent();
        }
        StringBuilder sbu = new StringBuilder(tablePath.toUri().toString());

        if (pathContainPartitionKey) {
            for (int index = 0; index < partitionKeys.size(); index++) {
                sbu.append(
                        File.separator + partitionKeys.get(index).getName() + "=" + partValues.get(
                                index));
            }
        } else {
            for (int index = 0; index < partitionKeys.size(); index++) {
                sbu.append(File.separator + partValues.get(
                        index));
            }
        }

        virSd.setLocation(sbu.toString());
        return virSd;
    }

    @Override
    public List<Partition> fetch(PartitionFetcher.Context<Partition> context) throws Exception {
        throw new UnsupportedOperationException(
                "HiveContinuousPartitionFetcher does not support fetch all partition.");
    }
}
