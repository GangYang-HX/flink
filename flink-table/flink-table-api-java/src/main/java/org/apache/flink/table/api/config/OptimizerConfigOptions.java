/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table planner module.
 *
 * <p>NOTE: All option keys in this class must start with "table.optimizer".
 */
@PublicEvolving
public class OptimizerConfigOptions {

    // ------------------------------------------------------------------------
    //  Optimizer Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> TABLE_OPTIMIZER_AGG_PHASE_STRATEGY =
            key("table.optimizer.agg-phase-strategy")
                    .stringType()
                    .defaultValue("AUTO")
                    .withDescription(
                            "Strategy for aggregate phase. Only AUTO, TWO_PHASE or ONE_PHASE can be set.\n"
                                    + "AUTO: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one"
                                    + " stage aggregate depends on cost. \n"
                                    + "TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate. "
                                    + "Note that if aggregate call does not support optimize into two phase, we will still use one stage aggregate.\n"
                                    + "ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Long> TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD =
            key("table.optimizer.join.broadcast-threshold")
                    .longType()
                    .defaultValue(1024 * 1024L)
                    .withDescription(
                            "Configures the maximum size in bytes for a table that will be broadcast to all worker "
                                    + "nodes when performing a join. By setting this value to -1 to disable broadcasting.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED =
            key("table.optimizer.distinct-agg.split.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Tells the optimizer whether to split distinct aggregation "
                                    + "(e.g. COUNT(DISTINCT col), SUM(DISTINCT col)) into two level. "
                                    + "The first aggregation is shuffled by an additional key which is calculated using "
                                    + "the hashcode of distinct_key and number of buckets. This optimization is very useful "
                                    + "when there is data skew in distinct aggregation and gives the ability to scale-up the job. "
                                    + "Default is false.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM =
            key("table.optimizer.distinct-agg.split.bucket-num")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Configure the number of buckets when splitting distinct aggregation. "
                                    + "The number is used in the first level aggregation to calculate a bucket key "
                                    + "'hash_code(distinct_key) % BUCKET_NUM' which is used as an additional group key after splitting.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED =
            key("table.optimizer.reuse-sub-plan-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will try to find out duplicated sub-plans and reuse them.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED =
            key("table.optimizer.reuse-source-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will try to find out duplicated table sources and "
                                    + "reuse them. This works only when "
                                    + TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED.key()
                                    + " is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED =
            key("table.optimizer.source.aggregate-pushdown-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will push down the local aggregates into "
                                    + "the TableSource which implements SupportsAggregatePushDown.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED =
            key("table.optimizer.source.predicate-pushdown-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will push down predicates into the FilterableTableSource. "
                                    + "Default value is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_JOIN_REORDER_ENABLED =
            key("table.optimizer.join-reorder-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables join reorder in optimizer. Default is disabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_MATERIALIZATION_ENABLED =
            key("table.optimizer.materialization-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables materialization in optimizer. Default is disabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_MATERIALIZATION_SUB_QUERY =
            key("table.optimizer.materialization.sub-query")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Sub query materialization in optimizer.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_PREFER_MATERIALIZATION =
            key("table.optimizer.prefer.materialization")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "prefer to use materialization in optimizer when exception or rules does not matched.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_TOPIC =
            key("table.optimizer.materialization.kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kafka topic name to send mv msg to.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_BOOTSTRAP =
            key("table.optimizer.materialization.kafka.bootstrap")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kafka topic bootstrap.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_DB_URL =
            key("table.optimizer.materialization.mysql.db.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("mysql url, to list all registered MVs.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_USER =
            key("table.optimizer.materialization.mysql.user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("mysql user.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_OPTIMIZER_MATERIALIZATION_MYSQL_PWD =
            key("table.optimizer.materialization.mysql.pwd")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("mysql pwd.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Long> TABLE_OPTIMIZER_MATERIALIZATION_QUERY_FLAG_START_TIME =
            key("table.optimizer.materialization.query.flag.start.time")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "window_start time, derived from window end time. to filter mv sql from hoodie manager");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Long> TABLE_OPTIMIZER_MATERIALIZATION_QUERY_FLAG_END_TIME =
            key("table.optimizer.materialization.query.flag.end.time")
                    .longType()
                    .noDefaultValue()
                    .withDescription("window_end time, query end time from user query sql.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Long> TABLE_OPTIMIZER_MATERIALIZATION_QUERY_DELAY_TIME =
            key("table.optimizer.materialization.query.delay.time")
                    .longType()
                    .defaultValue(360000L)
                    .withDescription("query delay time to wait hoodie manager finish init jobs.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED =
            key("table.optimizer.multiple-input-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will merge the operators with pipelined shuffling "
                                    + "into a multiple input operator to reduce shuffling and improve performance. Default value is true.");
}
