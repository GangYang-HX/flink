/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;


/** Rule to remove trivial {@link FlinkLogicalCalc}. */
public class FlinkLogicalCalcRemoveRule extends RelOptRule {
    //~ Static fields/initializers ---------------------------------------------

    public static final FlinkLogicalCalcRemoveRule INSTANCE =
            new FlinkLogicalCalcRemoveRule(RelFactories.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a CalcRemoveRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public FlinkLogicalCalcRemoveRule(RelBuilderFactory relBuilderFactory) {
        super(operand(FlinkLogicalCalc.class, any()), relBuilderFactory, null);
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        RexProgram program = calc.getProgram();
        if (!program.isTrivial()) {
            return;
        }
        RelNode input = calc.getInput();
        input = call.getPlanner().register(input, calc);
        call.transformTo(
                convert(
                        input,
                        calc.getTraitSet()));
    }
}
