package com.bilibili.bsql.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/** Util for calculating which kafka topic to sink to. */
public class KafkaSinkDistributedTP {
    private static final Random random = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkDistributedTP.class);

    public static int getPartition(int[] partitions, int parallelNumber, int parallelInstanceId) {
        int partitionLength = partitions.length;
        if (parallelNumber >= partitionLength) {
            int partitionNumber = partitions[parallelInstanceId % partitionLength];
            LOG.debug("use hash partition,to :{}", partitionNumber);
            return partitionNumber;
        } else {
            int partitionNumber =
                    partitions[calculate(parallelInstanceId, parallelNumber, partitionLength)];
            LOG.debug("use group partition, to:{}", partitionNumber);
            return partitionNumber;
        }
    }

    private static int calculate(int parallelInstanceId, int parallelNumber, int partitionLength) {
        int divisor = partitionLength / parallelNumber;
        int remainder = partitionLength % parallelNumber;
        if (parallelInstanceId < parallelNumber - remainder) {
            return random.nextInt(divisor) + parallelInstanceId * divisor;
        } else {
            return random.nextInt(divisor + 1)
                    + parallelInstanceId * divisor
                    + remainder
                    - (parallelNumber - parallelInstanceId);
        }
    }
}
