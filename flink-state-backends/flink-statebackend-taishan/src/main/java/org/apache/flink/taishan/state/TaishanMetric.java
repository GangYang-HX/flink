package org.apache.flink.taishan.state;

import com.bilibili.taishan.model.Record;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.taishan.state.cache.ByteArrayWrapper;
import org.apache.flink.taishan.state.cache.CacheConfiguration;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * @author Dove
 * @Date 2022/7/7 4:57 下午
 */
public class TaishanMetric {
	protected final Histogram writeHist;

	/** The lenth of the keys in taishan of the key */
	protected final Histogram writeKeyLength;

	/** The lenth of the values in taishan of the key */
	protected final Histogram writeValueLength;

	protected final Counter writeCounter;

	protected final Histogram deleteHist;

	protected final Histogram deleteKeyLength;

	protected final Counter deleteCounter;

	protected final Histogram readHist;

	/** The cost of taishan when there is null. */
	protected final Histogram readNull;

	/** The lenth of the keys in taishan of the key */
	protected final Histogram readKeyLength;

	/** The lenth of the values in taishan of the key */
	protected final Histogram readValueLength;

	/** The lenth of the values in taishan of the key */
	protected final Counter readCounter;

	protected final Counter readNullCounter;

	protected final Counter bloomNullCounter;

	/** The cost of taishan when iterator seek. */
	protected final Histogram seekHist;

	protected final Counter seekCounter;

	/** The cost of taishan when iterator next. */
	protected final Histogram nextHist;

	protected final Counter nextCounter;

	protected final Histogram batchWriteHist;

	protected final Counter batchWriteCounter;

	protected final Histogram batchDeleteHist;

	protected final Counter batchDeleteCounter;

	public final Counter cacheReadCounter;
	private final MetricGroup metricGroup;

	/** The controler of the catching rate of taishan's cost */
	public long loopLock;

	public boolean catchSample;

	protected boolean batchCatchSample;

	/** The controler of the bactch catching rate of taishan's cost */
	public long batchLoopLock;

	protected int samplingRate;

	private long bloomReadNullCount = 0L;
	private long allReadNullCount = 0L;

	private long cacheReadCount = 0L;
	private long allReadCount = 0L;

	public TaishanMetric(MetricGroup metricGroup, int samplingRate, CacheConfiguration cacheConfiguration) {
		this.writeHist = metricGroup.histogram("writeHist", new DescriptiveStatisticsHistogram(1000));
		this.writeKeyLength = metricGroup.histogram("writeKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.writeValueLength = metricGroup.histogram("writeValueLength", new DescriptiveStatisticsHistogram(1000));
		this.writeCounter = metricGroup.counter("writeCounter");
		this.deleteHist = metricGroup.histogram("deleteHist", new DescriptiveStatisticsHistogram(1000));
		this.deleteKeyLength = metricGroup.histogram("deleteKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.deleteCounter = metricGroup.counter("deleteCounter");
		this.readHist = metricGroup.histogram("readHist", new DescriptiveStatisticsHistogram(1000));
		this.readNull = metricGroup.histogram("readNull", new DescriptiveStatisticsHistogram(1000));
		this.readKeyLength = metricGroup.histogram("readKeyLength", new DescriptiveStatisticsHistogram(1000));
		this.readValueLength = metricGroup.histogram("readValueLength", new DescriptiveStatisticsHistogram(1000));
		this.readCounter = metricGroup.counter("readCounter");
		this.readNullCounter = metricGroup.counter("readNullCounter");
		this.bloomNullCounter = metricGroup.counter("bloomNullCounter");
		this.seekHist = metricGroup.histogram("seekHist", new DescriptiveStatisticsHistogram(1000));
		this.seekCounter = metricGroup.counter("seekCounter");
		this.nextHist = metricGroup.histogram("nextHist", new DescriptiveStatisticsHistogram(1000));
		this.nextCounter = metricGroup.counter("nextCounter");

		this.batchWriteHist = metricGroup.histogram("batchWriteHist", new DescriptiveStatisticsHistogram(1000));
		this.batchDeleteHist = metricGroup.histogram("batchDeleteHist", new DescriptiveStatisticsHistogram(1000));
		this.batchWriteCounter = metricGroup.counter("batchWriteCounter");
		this.batchDeleteCounter = metricGroup.counter("batchDeleteCounter");
		this.cacheReadCounter = metricGroup.counter("cacheReadCounter");
		this.samplingRate = samplingRate;
		this.loopLock = 0L;
		this.batchLoopLock = 0L;
		this.metricGroup = metricGroup;
		registerMetric(metricGroup, cacheConfiguration);
	}

	public static TaishanMetric getOrCreateTaishanMetric(Map<String, TaishanMetric> stateTaishanMetricMap,
														 String stateName,
														 MetricGroup metricGroup,
														 int samplingRate,
														 CacheConfiguration cacheConfiguration) {
		TaishanMetric taishanMetric;
		if (stateTaishanMetricMap.containsKey(stateName)) {
			taishanMetric = stateTaishanMetricMap.get(stateName);
		} else {
			MetricGroup metric = metricGroup.addGroup(stateName);
			taishanMetric = new TaishanMetric(metric, samplingRate, cacheConfiguration);
		}
		return taishanMetric;
	}

	public MetricGroup getMetricGroup() {
		return metricGroup;
	}

	private void registerMetric(MetricGroup metricGroup, CacheConfiguration cacheConfiguration) {
		if (cacheConfiguration.getBloomFilterEnabled()) {
			metricGroup.gauge("state.bloom.filter.rate", () -> allReadNullCount == 0 ? 1.0 : Long.valueOf(bloomReadNullCount).doubleValue() / allReadNullCount);
		}
		if (cacheConfiguration.isCacheEnable()) {
			metricGroup.gauge("state.cache.rate", () -> allReadCount == 0 ? 1.0 : Long.valueOf(cacheReadCount).doubleValue() / allReadCount);
		}
	}

	public long updateCatchingRate() {
		int x = 100 - samplingRate;
		catchSample = (loopLock + x) % 100 >= x;
		if (catchSample) {
			return System.nanoTime();
		} else {
			return 0;
		}
	}

	public long updateBatchCatchingRate() {
		int x = 100 - samplingRate;
		batchCatchSample = (batchLoopLock + x) % 100 >= x;
		if (batchCatchSample) {
			return System.nanoTime();
		} else {
			return 0;
		}
	}

	public void takeReadMetrics(long start, byte[] key, @Nullable byte[] value) {
		if (value != null) {
			if (catchSample) {
				long end = System.nanoTime();
				this.readHist.update(end - start);
				this.readKeyLength.update(key.length);
				this.readValueLength.update(value.length);
			}
			this.readCounter.inc();
			this.allReadCount++;
		} else {
			if (catchSample) {
				long end = System.nanoTime();
				this.readNull.update(end - start);
			}
			this.readNullCounter.inc();
			this.allReadNullCount++;
		}
		this.loopLock++;
	}

	public void takeWriteMetrics(long start, byte[] key, @Nullable byte[] value) {
		if (catchSample) {
			long end = System.nanoTime();
			this.writeHist.update(end - start);
			this.writeKeyLength.update(key.length);
			if (value != null) {
				this.writeValueLength.update(value.length);
			}
		}
		this.writeCounter.inc();
		this.loopLock++;
	}

	public void takeWriteBatchMetrics(long start, Map<ByteArrayWrapper, Record> records) {
		if (batchCatchSample) {
			long end = System.nanoTime();
			long avgTime = (end - start) / records.size();

			for (Record record : records.values()) {
				this.writeHist.update(avgTime);
				this.writeKeyLength.update(record.getKey().length);
				if (record.getValue() != null) {
					this.writeValueLength.update(record.getValue().length);
				}
			}
			this.batchWriteHist.update(end - start);
		}
		this.writeCounter.inc(records.size());
		this.batchWriteCounter.inc();
		this.batchLoopLock++;
	}

	public void takeDeleteMetrics(long start, byte[] key) {
		if (catchSample) {
			long end = System.nanoTime();
			this.deleteHist.update(end - start);
			this.deleteKeyLength.update(key.length);
		}
		this.deleteCounter.inc();
		this.loopLock++;
	}

	public void takeDeleteBatchMetrics(long start, Set<ByteArrayWrapper> keyList) {
		if (batchCatchSample) {
			long end = System.nanoTime();
			long avgTime = (end - start) / keyList.size();
			for (ByteArrayWrapper bytes : keyList) {
				this.deleteHist.update(avgTime);
				this.deleteKeyLength.update(bytes.array().length);
			}
			this.batchDeleteHist.update(end - start);
		}
		this.batchDeleteCounter.inc();
		this.deleteCounter.inc(keyList.size());
		this.batchLoopLock++;
	}

	public void takeSeekMetrics(long start) {
		if (catchSample) {
			long end = System.nanoTime();
			this.seekHist.update(end - start);
		}
		this.seekCounter.inc();
		this.loopLock++;
	}

	public void takeNextMetrics(long start) {
		if (catchSample) {
			long end = System.nanoTime();
			this.seekHist.update(end - start);
		}
		this.nextCounter.inc();
		this.loopLock++;
	}

	public void takeBloomFilterNull() {
		this.bloomNullCounter.inc();
		this.bloomReadNullCount++;
		this.allReadNullCount++;
	}

	public void takeCacheReadMetrics(byte[] value) {
		if (value != null) {
			cacheReadCounter.inc();
			this.cacheReadCount++;
			this.allReadCount++;
		}
	}
}
