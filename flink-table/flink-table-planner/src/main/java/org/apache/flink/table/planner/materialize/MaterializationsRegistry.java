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

import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** MaterializationsRegistry. */
public class MaterializationsRegistry {

    private List<RelOptMaterialization> cache;

    private final CatalogManager catalogManager;

    private final PlannerBase planner;

    public MaterializationsRegistry(CatalogManager catalogManager, PlannerBase planner) {
        this.catalogManager = catalogManager;
        this.planner = planner;
        reload();
        ScheduledExecutorService pool =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("MaterializationsRegistry-Update-%d")
                                .build());
        pool.scheduleWithFixedDelay(() -> reload(), 10, 10, TimeUnit.MINUTES);
    }

    public synchronized List<RelOptMaterialization> getAllMaterializations() {
        return new ArrayList<>(cache);
    }

    private void reload() {
        Map<ObjectIdentifier, String> catalogMvList =
                catalogManager.getAllMaterializedViewObjectsForRewriting();
        List<RelOptMaterialization> tmpCache = new ArrayList<>(catalogMvList.size());
        for (Map.Entry<ObjectIdentifier, String> view : catalogMvList.entrySet()) {
            FlinkRelOptMaterialization mv = createMaterialization(view.getKey(), view.getValue());
            if (mv == null) {
                continue;
            }
            tmpCache.add(mv);
        }
        cache = tmpCache;
    }

    public FlinkRelOptMaterialization createMaterialization(ObjectIdentifier id, String view) {

        String query = view;
        List<Operation> op = planner.getParser().parse(query);
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) op.get(0);
        RelNode queryRelNode = queryOperation.getCalciteTree();

        Optional<ContextResolvedTable> contextResolvedTable = catalogManager.getTable(id);
        if (!contextResolvedTable.isPresent()) {
            System.out.println("id is null: " + id);
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
        return new FlinkRelOptMaterialization(tableRelNode, queryRelNode);
    }
}
