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

package org.apache.flink.table.planner.materialize;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** MaterializationsRegistry. */
public class MaterializationsRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaterializationsRegistry.class);

    private final Map<ObjectIdentifier, RelOptMaterialization> cache = new ConcurrentHashMap<>();

    private final CatalogManager catalogManager;

    private final PlannerBase planner;
    private final ScheduledExecutorService pool =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("MaterializationsRegistry-Update-%d")
                            .build());

    private static final ThreadFactory NAMED_THREAD_FACTORY =
            new ThreadFactoryBuilder()
                    .setNameFormat("materialization-reload-%d")
                    .setDaemon(true)
                    .build();
    private static final ExecutorService EXECUTOR_SERVICE =
            new ThreadPoolExecutor(
                    10,
                    10,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(100),
                    NAMED_THREAD_FACTORY,
                    new ThreadPoolExecutor.AbortPolicy());

    public MaterializationsRegistry(CatalogManager catalogManager, PlannerBase planner) {
        LOGGER.info("init MaterializationsRegistry");
        this.catalogManager = catalogManager;
        this.planner = planner;
        reload();
        pool.scheduleWithFixedDelay(this::reload, 10, 10, TimeUnit.MINUTES);
        registerShutdownHook();
    }

    public synchronized List<RelOptMaterialization> getAllMaterializations() {
        return new ArrayList<>(cache.values());
    }

    private void reload() {
        LOGGER.info("reload starts...");
        List<Tuple3<ObjectIdentifier, String, Long>> catalogMvList =
                catalogManager.getAllMaterializedViewObjectsForRewriting(null);
        LOGGER.info("reload materialize size: {}", catalogMvList.size());

        // update cache
        CountDownLatch latch = new CountDownLatch(catalogMvList.size());
        for (Tuple3<ObjectIdentifier, String, Long> view : catalogMvList) {
            EXECUTOR_SERVICE.execute(
                    () -> {
                        try {
                            if (!cache.containsKey(view.f0)) {
                                FlinkRelOptMaterialization mv =
                                        createMaterialization(view.f0, view.f1, view.f2);
                                if (mv != null) {
                                    cache.put(view.f0, mv);
                                }
                            }
                            LOGGER.info(
                                    "init materialize success,table: {}, query: {}, ctime: {}",
                                    view.f0,
                                    view.f1,
                                    view.f2);
                        } catch (Exception e) {
                            LOGGER.error(
                                    "failed to execute update materialization task in reload method.");
                        } finally {
                            latch.countDown();
                        }
                    });
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) {
            LOGGER.error("InterruptedException in reload method.");
        }
        LOGGER.info("reload finished.");
    }

    public FlinkRelOptMaterialization createMaterialization(
            ObjectIdentifier id, String view, Long ctime) {
        List<Operation> op = planner.getParser().parse(view);
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) op.get(0);
        RelNode queryRelNode = queryOperation.getCalciteTree();

        Optional<ContextResolvedTable> contextResolvedTable = catalogManager.getTable(id);
        if (!contextResolvedTable.isPresent()) {
            LOGGER.warn("id is null: " + id);
            return null;
        }

        ContextResolvedTable mvTable = contextResolvedTable.get();
        FlinkStatistic flinkStatistic = FlinkStatistic.unknown(mvTable.getResolvedSchema()).build();
        CatalogSchemaTable catalogSchemaTable =
                new CatalogSchemaTable(mvTable, flinkStatistic, false);
        CatalogSourceTable sourceTable =
                new CatalogSourceTable(
                        planner.getRelBuilder().getRelOptSchema(),
                        id.toList(),
                        catalogSchemaTable.getRowType(planner.getTypeFactory()),
                        catalogSchemaTable);

        RelOptTable.ToRelContext context =
                ViewExpanders.simpleContext(planner.getRelBuilder().getCluster());
        RelNode tableRelNode = sourceTable.toRel(context);
        return new FlinkRelOptMaterialization(tableRelNode, queryRelNode, ctime);
    }

    public void close() {
        LOGGER.info("close MaterializationsRegistry pool.");
        try {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("close MaterializationsRegistry pool failed.", e);
        } finally {
            try {
                pool.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    private void destroy() {
        LOGGER.info("close MaterializationsRegistry EXECUTOR_SERVICE pool.");
        try {
            if (!EXECUTOR_SERVICE.isShutdown()) {
                EXECUTOR_SERVICE.shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("close EXECUTOR_SERVICE pool failed.", e);
        } finally {
            try {
                if (!EXECUTOR_SERVICE.isShutdown()) {
                    EXECUTOR_SERVICE.shutdown();
                }
            } catch (Exception ignored) {
            }
        }
    }

    private void registerShutdownHook() {
        Thread shutdownHook = new Thread(this::close);
        Thread shutdownStaticHook = new Thread(this::destroy);
        ShutdownHookUtil.addShutdownHookThread(
                shutdownHook, MaterializationsRegistry.class.getSimpleName(), LOGGER);
        ShutdownHookUtil.addShutdownHookThread(
                shutdownStaticHook, MaterializationsRegistry.class.getSimpleName(), LOGGER);
    }
}
