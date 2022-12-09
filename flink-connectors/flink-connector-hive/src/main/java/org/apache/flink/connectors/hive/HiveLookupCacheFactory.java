package org.apache.flink.connectors.hive;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HiveLookupCacheFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveLookupCacheFactory.class);

    private ScheduledExecutorService asyncScheduler = Executors.newScheduledThreadPool(2);
    private volatile boolean ready = false; //Indicates whether the dimension table data is ready

    /**
     * Synchronized is used to block multi-threaded access to ensure that the current method is thread-safe
     *
     * @param loadJob
     * @param reloadInterval
     */
    public synchronized void initCache(final Runnable loadJob, final Duration reloadInterval) {
        if (ready) {
            return;
        }
        try {
            Future future = asyncScheduler.submit(loadJob);
            future.get(
                    reloadInterval.toMillis(),
                    TimeUnit.MILLISECONDS); //Load cached data for the first time by synchronously
            ready = true;
        } catch (Exception e) {
            throw new RuntimeException("load hive look up data to cache is failed,{}", e);
        }
        asyncScheduler.scheduleAtFixedRate(
                loadJob,
                reloadInterval.toMillis(),
                reloadInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        LOG.info(
                "Asynchronous load scheduler initialization succeeded，reloadInterval={}",
                reloadInterval.toString());
    }

    public static HiveLookupCacheFactory getInstance() {
        return HiveLookupCacheFactoryInstance.INSTANCE;
    }

    private static class HiveLookupCacheFactoryInstance {
        private static final HiveLookupCacheFactory INSTANCE = new HiveLookupCacheFactory();
    }
}
