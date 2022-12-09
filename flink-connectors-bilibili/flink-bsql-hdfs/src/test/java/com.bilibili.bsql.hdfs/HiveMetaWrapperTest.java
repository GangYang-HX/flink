package com.bilibili.bsql.hdfs;

import com.bilibili.bsql.hdfs.util.HiveMetaWrapper;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: JinZhengyu
 * @Date: 2022/8/3 上午11:26
 */
@NoArgsConstructor
@lombok.Data
@Slf4j
public class HiveMetaWrapperTest {


    @Test
    public void getRecentReadyPartition() {

        List<String> partitions = new ArrayList<>();
        partitions.add("log_date");
//		partitions.add("log_hour");
        try {
            HiveMetaWrapper.HiveRecentPartitionReq req = HiveMetaWrapper.HiveRecentPartitionReq.builder()
                    .tableName("b_dwm.live_dwd_prty_memb_rel_spm_record_l_d")
                    .partition(partitions)
                    .build();
//			HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition partition = HiveMetaWrapper.getRecentReadyPartition(req);
//      		System.out.println(partition);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    @Test
    public void getRecentPartitions() {

        List<String> partitions = new ArrayList<>();
        partitions.add("log_date");
        partitions.add("log_hour");
        try {
            HiveMetaWrapper.HiveRecentPartitionReq req = HiveMetaWrapper.HiveRecentPartitionReq.builder()
                    .tableName("b_ods.rods_db1742_t_campus_users_a_hr")
                    .partition(partitions)
                    .build();
			/*List<HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition> recentPartitions = HiveMetaWrapper.getRecentPartitions(req);
      		for (HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition partition:recentPartitions) {
        		System.out.println(partition.toString());
      		}*/
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void getMetaData() {
        try {
            HiveMetaWrapper.HiveMetaDetail metaData = HiveMetaWrapper.getMetaData("b_dwm", "live_dwd_prty_memb_rel_spm_record_l_d");
            log.info(metaData.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
