package com.bilibili.bsql.kafka.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author weizefeng
 * @Date 2022/3/8 14:31
 **/
@Slf4j
public class ZkValidator {
    public static void validatorZk(String zkHost) {
        int retryCount = 0;
        ZooKeeper zk;
        while (true) {
            try {
                zk = new ZooKeeper(zkHost, 10000, (event) -> {
                });
                TimeUnit.SECONDS.sleep(1);
                ZooKeeper.States state = zk.getState();
                if (state.isConnected()) {
                    zk = null;
                    log.info("connect zk success, zkHost = {}", zkHost);
                    break;
                } else {
                    if (retryCount > 2) {
                        throw new RuntimeException("cannot connect zookeeper = "
                            + zkHost + ", please check zookeeper host or set blacklist.enable = false");

                    }
                }
            } catch (Exception e) {
                log.error("validatorZk occur error, ", e);
                throw new RuntimeException("cannot connect zookeeper = "
                    + zkHost + ", please check zookeeper host or set blacklist.enable = false");
            }
            retryCount++;
        }
    }
}
