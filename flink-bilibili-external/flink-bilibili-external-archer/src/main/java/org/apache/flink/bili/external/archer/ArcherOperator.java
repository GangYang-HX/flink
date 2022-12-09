package org.apache.flink.bili.external.archer;

import javafx.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.bili.external.archer.constant.ArcherConstants;
import org.apache.flink.bili.external.archer.constant.SymbolConstants;
import org.apache.flink.bili.external.archer.integrate.*;
import org.apache.flink.bili.external.archer.utils.PartTimeUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.bili.external.archer.constant.ArcherConstants.*;


/**
 * @author: zhuzhengjun
 * @date: 2021/12/6 5:15 下午
 */
public class ArcherOperator {
	private static final Logger LOG = LoggerFactory.getLogger(ArcherOperator.class);


	private final Properties properties;
	private static String CREATE_USER = "";
	private static String OP_USER = "";
	private static String LOG_HOUR = "log_hour";
	private static final DateTimeFormatter BIZ_DATE_TIME_FORMATTER =
		DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private final Map<Tuple2<String, String>, Long> tableInfoMappingToJobIdInfo;

	private static final Object LOCK = new Object();

	public ArcherOperator(Properties properties) {
		this.properties = properties;
		this.tableInfoMappingToJobIdInfo = new ConcurrentHashMap<>();
	}

	public void open() {
		List<Pair<String, String>> result = getTableInfo(properties);
		registerTableListToArcherIfNotExist(result);
		LOG.info("register {} to archer successful. ", result);
	}

	private List<Pair<String, String>> getTableInfo(Properties properties) {
		String database = properties.getProperty(SINK_DATABASE_NAME_KEY);
		String tableInfos = properties.getProperty(SINK_TABLE_NAME_KEY);
		String commitUserId = properties.getProperty(SYSTEM_USER_ID);
		CREATE_USER = commitUserId;
		OP_USER = commitUserId;

		return Arrays.stream(tableInfos.split(","))
			.map(table -> new Pair<>(database, table))
			.collect(Collectors.toList());
	}

	private void registerTableListToArcherIfNotExist(List<Pair<String, String>> databaseTablesPair) {
		Preconditions.checkNotNull(databaseTablesPair, "databaseTablesPair info can't be null");
		databaseTablesPair.forEach(
			pair -> {
				String database = pair.getKey();
				String table = pair.getValue();
				Long jobId = registerSingleTableToArcherIfNotExist(database, table);
				if (jobId == null) {
					throw new RuntimeException(
						"registerToArcher failed with null jobId returned!");
				}
				LOG.info("jobId is: {}", jobId);
				tableInfoMappingToJobIdInfo.putIfAbsent(Tuple2.of(database, table), jobId);
			});
	}


	private Long registerSingleTableToArcherIfNotExist(String database, String table) {
		Preconditions.checkNotNull(database, "database cannot be null!");
		Preconditions.checkNotNull(table, "table cannot be null!");
		String jobName = getJobName(database, table);
		String fullTableName = getFullName(database, table);
		String relateUniqueUid = getUniqueJobRelateUid(fullTableName);

		Long jobId = null;

		synchronized (LOCK) {
			ArcherMessage<ArcherGetJobIdByRelateUIDResp> getJobIdByRelateUIDResp =
				ArcherHandler.getJobIdByRelateUID(relateUniqueUid);

			if (!ArcherMessage.isSuccessful(getJobIdByRelateUIDResp)) {
				LOG.error(
					"getJobIdByRelateUID: {} failed: {}",
					relateUniqueUid,
					getJobIdByRelateUIDResp);
				return jobId;
			}
			List<Long> jobIds =
				Objects.isNull(getJobIdByRelateUIDResp.getData())
					? null
					: getJobIdByRelateUIDResp.getData().getJobIds();
			if (CollectionUtils.isEmpty(jobIds)) {
				// at first time, to create job
				ArcherMessage<ArcherCreateDummyJobResp> createDummyJobResp =
					ArcherHandler.createDummyJob(
						jobName,
						fullTableName,
						relateUniqueUid,
						properties.get(SINK_PARTITION_KEY).toString().contains(LOG_HOUR) ?
							ArcherConstants.JOB_CRON_LOG_HOUR : JOB_CRON_LOG_DAY,
						CREATE_USER,
						OP_USER);
				if (ArcherMessage.isSuccessful(createDummyJobResp)
					&& Objects.nonNull(createDummyJobResp.getData())) {
					jobId = createDummyJobResp.getData().getJobId();
					return jobId;
				} else {
					LOG.error(
						"jobIdByRelateUid {} is empty but create failed: {} !",
						relateUniqueUid,
						createDummyJobResp);
				}
			} else {
				if (jobIds.size() > 1) {
					LOG.warn(
						"jobIds num for relateUniqueUid: {} exceeds one! {}",
						relateUniqueUid,
						jobIds);
				}
				jobId = jobIds.get(0);
			}
		}

		return jobId;
	}


	public boolean updatePartitionStatus(String database, String table,List<String> partitionKeys, List<String> partitionValues) throws Exception {
		Long jobId = tableInfoMappingToJobIdInfo.get(Tuple2.of(database, table));
		if (jobId == null) {
			LOG.warn(
				"cannot find jobId: {} from tableInfoMappingToJobIdInfo: {}",
				Tuple2.of(database, table),
				tableInfoMappingToJobIdInfo);
			return false;
		}
		Tuple2<String, String> bizTimeTuple = getPartitionTime(partitionKeys, partitionValues);
		if (bizTimeTuple == null) {
			LOG.warn("failed to format bizTimeTuple for {}", partitionValues);
			return false;
		}
		if (updatePartitionStatusToArcher(jobId, bizTimeTuple)) {
			LOG.info(
				"update complete partition {} to archer for {}.{} successful!",
				bizTimeTuple,
				database,
				table);
			return true;
		} else {
			LOG.warn(
				"update complete partition {} to archer for {}.{} failed! ",
				bizTimeTuple,
				database,
				table);
			return false;
		}

	}

	private Tuple2<String, String> getPartitionTime(List<String> partitionKeys, List<String> partitionValues) throws Exception {
		LocalDateTime bizStartDayTime = null, bizEndDayTime = null;
		if (CollectionUtils.isNotEmpty(partitionValues) && CollectionUtils.isNotEmpty(partitionKeys)) {
			int size = PartTimeUtils.containsKey(partitionKeys);
			// day
			if (size == 1) {
				bizStartDayTime = PartTimeUtils.getDayLocalDateTime(partitionValues.get(0));
				bizEndDayTime = bizStartDayTime.plusDays(1).minusSeconds(1);
			}
			// day + hour
			else if (size == 2) {
				bizStartDayTime =
					PartTimeUtils.getHourLocalDateTime(
						partitionValues.get(0), partitionValues.get(1));
				bizEndDayTime = bizStartDayTime.plusHours(1).minusSeconds(1);
			}
		}

		return bizStartDayTime == null
			? null
			: Tuple2.of(
			bizStartDayTime.format(BIZ_DATE_TIME_FORMATTER),
			bizEndDayTime.format(BIZ_DATE_TIME_FORMATTER));
	}

	private boolean updatePartitionStatusToArcher(Long jobId, Tuple2<String, String> bizTimeTuple) {
		Preconditions.checkNotNull(jobId, "jobId cannot be null!");
		Preconditions.checkNotNull(bizTimeTuple, "bizTimeTuple cannot be null!");
		ArcherMessage<ArcherQueryInstanceResp> archerQueryInstanceResp =
			ArcherHandler.queryInstance(
				String.valueOf(jobId), bizTimeTuple.f0, bizTimeTuple.f1, OP_USER);
		if (!ArcherMessage.isSuccessful(archerQueryInstanceResp)
			|| Objects.isNull(archerQueryInstanceResp.getData())) {
			LOG.warn("queryInstance failed with return message {}", archerQueryInstanceResp);
			return false;
		} else if (ArcherMessage.isSuccessful(archerQueryInstanceResp) && CollectionUtils.isEmpty(archerQueryInstanceResp.getData().getInstances())) {
			LOG.warn("instance is empty while update partitions status, will return true instead. job id :{} ,bizTimeTuple:{}", jobId, bizTimeTuple);
			return true;
		}
		List<InstanceQuery> archerQueryInstanceRespData =
			archerQueryInstanceResp.getData().getInstances();
		if (archerQueryInstanceRespData.size() > 1) {
			LOG.warn(
				"jobId: {}, bizTimeTuple: {} get instances: {} more than one!",
				jobId,
				bizTimeTuple,
				archerQueryInstanceRespData);
		}
		InstanceQuery instance = archerQueryInstanceRespData.get(0);
		if (StringUtils.isBlank(instance.getInstanceId())) {
			LOG.warn(
				"queryInstance got blank instanceId: {} for params jobId: {}, bizTimeTuple: {}",
				instance,
				jobId,
				bizTimeTuple);
			return false;
		}

		ArcherMessage<ArcherUpdateInstanceSuccessResp> updatePartitionInstanceSuccess =
			ArcherHandler.updatePartitionInstanceSuccess(instance.getInstanceId(), OP_USER);
		if (!ArcherMessage.isSuccessful(updatePartitionInstanceSuccess)) {
			LOG.warn(
				"updatePartitionInstanceSuccess failed with return code {}",
				updatePartitionInstanceSuccess.getCode());
			return false;
		}

		return true;
	}

	public boolean updateInstanceStatusByRelateUID(String relateUID, List<String> partitionKeys, List<String> partitionValues) throws Exception {
		Preconditions.checkNotNull(relateUID, "relateUID cannot be null!");
		Preconditions.checkNotNull(partitionKeys, "partitionKeys cannot be null!");
		Preconditions.checkNotNull(partitionValues, "partitionValues cannot be null!");

		Tuple2<String, String> bizTimeTuple = getPartitionTime(partitionKeys, partitionValues);
		ArcherMessage<ArcherQueryInstanceResp> archerQueryInstanceResp = ArcherHandler.queryInstanceByRelateUID(
				relateUID, bizTimeTuple.f0, bizTimeTuple.f1, USER);
		if (!ArcherMessage.isSuccessful(archerQueryInstanceResp) || Objects.isNull(archerQueryInstanceResp.getData())) {
			LOG.warn("queryInstance failed with return message {}", archerQueryInstanceResp);
			return false;
		} else if (ArcherMessage.isSuccessful(archerQueryInstanceResp) && CollectionUtils.isEmpty(archerQueryInstanceResp.getData().getInstances())) {
			LOG.warn("instance is empty while update partitions status, will return true instead. relateUID id :{} ,bizTimeTuple:{}", relateUID, bizTimeTuple);
			return true;
		}
		List<InstanceQuery> archerQueryInstanceRespData = archerQueryInstanceResp.getData().getInstances();
		if (archerQueryInstanceRespData.size() > 1) {
			LOG.warn("relateUID: {}, bizTimeTuple: {} get instances: {} more than one!",
					relateUID,
					bizTimeTuple,
					archerQueryInstanceRespData);
		}
		InstanceQuery instance = archerQueryInstanceRespData.get(0);
		if (StringUtils.isBlank(instance.getInstanceId())) {
			LOG.warn(
					"queryInstance got blank instanceId: {} for params relateUID: {}, bizTimeTuple: {}",
					instance,
					relateUID,
					bizTimeTuple);
			return false;
		}

		ArcherMessage<ArcherUpdateInstanceSuccessResp> updatePartitionInstanceSuccess =
				ArcherHandler.updatePartitionInstanceSuccess(instance.getInstanceId(), USER);
		if (!ArcherMessage.isSuccessful(updatePartitionInstanceSuccess)) {
			LOG.warn(
					"updatePartitionInstanceSuccess failed with return code {}",
					updatePartitionInstanceSuccess.getCode());
			return false;
		}

		return true;
	}

	private static String getJobName(String database, String table) {
		return database + SymbolConstants.POINT + table;
	}

	private static String getFullName(String database, String table) {
		return database + SymbolConstants.POINT + table;
	}

	private static String getUniqueJobRelateUid(String jobName) {
		return jobName;
	}


}
