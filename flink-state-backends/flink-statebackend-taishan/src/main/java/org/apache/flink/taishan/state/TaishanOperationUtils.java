package org.apache.flink.taishan.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.taishan.state.cache.CacheConfiguration;
import org.apache.flink.util.function.LongFunctionWithException;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Dove
 * @Date 2022/10/11 2:33 下午
 */
public class TaishanOperationUtils {
	private static final String MANAGED_MEMORY_RESOURCE_ID = "state-taishan-managed-memory";

	@Nullable
	public static OpaqueMemoryResource<TaishanSharedResources> allocateSharedCachesIfConfigured(
		MemoryManager memoryManager,
		double memoryFraction,
		CacheConfiguration cacheConfiguration,
		ExecutionConfig executionConfig,
		MetricGroup metricGroup,
		int numberOfKeyGroups,
		Logger logger) throws IOException {

		if (!cacheConfiguration.isOffHeap()) {
			return null;
		}

		final double writeBufferRatio = cacheConfiguration.getWriteBufferRatio();

		final LongFunctionWithException<TaishanSharedResources, Exception> allocator = (size) ->
			TaishanMemoryControllerUtils.allocateTaishanSharedResources(
				size,
				writeBufferRatio,
				executionConfig,
				numberOfKeyGroups,
				cacheConfiguration,
				metricGroup
			);

		try {
			logger.info("Getting managed memory shared cache for Taishan, type:{}.", MANAGED_MEMORY_RESOURCE_ID);
			return memoryManager.getSharedMemoryResourceForManagedMemory(MANAGED_MEMORY_RESOURCE_ID, allocator, memoryFraction);
		} catch (Exception e) {
			throw new IOException("Failed to acquire shared cache resource for Taishan", e);
		}
	}
}
