package org.apache.flink.taishan.state.cache;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.*;

public class CacheBufferGroup implements Serializable {

	private static final long serialVersionUID = 1L;
	public ConcurrentMap<String, SnapshotableCacheClient> taishanClientMap = new ConcurrentHashMap<>();
	public List<HeapStatusMonitor.MonitorResult> monitorResultList = new CopyOnWriteArrayList<>();
	public List<List<CacheResult>> cacheResultList = new CopyOnWriteArrayList<>();
	public CacheBufferGroup(CacheConfiguration cacheConfiguration) {
		/*
		if (cacheConfiguration.isDynamicCache()) {
			HeapStatusMonitor heapStatusMonitor = new HeapStatusMonitor(60);
			ScheduledExecutorService heapStatusMonitorThreadPool = Executors.newScheduledThreadPool(1);
			heapStatusMonitorThreadPool.scheduleAtFixedRate(new Runnable() {
																@Override
																public void run() {
																	heapStatusMonitor.runCheck();
																	monitorResultList.add(heapStatusMonitor.getMonitorResult());
																}
															}, 1L,
				1L,
				TimeUnit.SECONDS);
			ScheduledExecutorService cacheMonitorThreadPool = Executors.newScheduledThreadPool(1);
			cacheMonitorThreadPool.scheduleAtFixedRate(new Runnable() {
														   @Override
														   public void run() {
															   List<CacheResult> list = new CopyOnWriteArrayList<>();
															   for (Map.Entry<String, SnapshotableCacheClient> entry : rocksDBClientMap.entrySet()) {
																   CacheBufferClient cachedRocksDBClient = (CacheBufferClient) entry.getValue();
																   list.add(new CacheResult(cachedRocksDBClient.cache.asMap().size()));
															   }
															   cacheResultList.add(list);
														   }
													   },
				1L,
				1L,
				TimeUnit.SECONDS);
		}

		 */

	}

	public void addCacheTaishanClient(String stateName, SnapshotableCacheClient taishanClient) {
		taishanClientMap.put(stateName, taishanClient);
	}

	public SnapshotableCacheClient getCacheClient(String stateName) {
		return taishanClientMap.get(stateName);
	}
}
class CacheResult {
	public long size;

	public CacheResult(long size) {
		this.size = size;
	}

}
