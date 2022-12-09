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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Base class for runtime implementation represented as {@link AggregateFunction} that is
 * constructed from {@link BuiltInFunctionDefinition#specialize(SpecializedContext)}.
 *
 * <p>Subclasses must offer a constructor that takes {@link SpecializedContext} if they are
 * constructed from a {@link BuiltInFunctionDefinition}. Otherwise the {@link
 * #BuiltInAggregateFunction()} constructor might be more appropriate.
 *
 * <p>By default, all built-in functions work on internal data structures. However, this can be
 * changed by overriding {@link #getArgumentDataTypes()}, {@link #getAccumulatorDataType()}, and
 * {@link #getOutputDataType()}. Or by overriding {@link #getTypeInference(DataTypeFactory)}
 * directly.
 *
 * <p>Since the accumulator type is runtime specific, it must be declared explicitly; otherwise it
 * is derived from the output type.
 */
@Internal
public abstract class BuiltInAggregateFunction<T, ACC> extends AggregateFunction<T, ACC> {

	// can be null if a Calcite function definition is the origin
	private transient @Nullable BuiltInFunctionDefinition definition;

	private transient List<DataType> argumentDataTypes;

	private transient DataType outputDataType;

//	protected BuiltInAggregateFunction(
//		BuiltInFunctionDefinition definition, SpecializedContext context) {
//		this.definition = definition;
//		final CallContext callContext = context.getCallContext();
//		argumentDataTypes =
//			callContext.getArgumentDataTypes().stream()
//				.map(DataTypeUtils::toInternalDataType)
//				.collect(Collectors.toList());
//		outputDataType =
//			callContext
//				.getOutputDataType()
//				.map(DataTypeUtils::toInternalDataType)
//				.orElseThrow(IllegalStateException::new);
//	}

	protected BuiltInAggregateFunction() {
		// for overriding the required methods manually
	}

	public List<DataType> getArgumentDataTypes() {
		Preconditions.checkNotNull(argumentDataTypes, "Argument data types not set.");
		return argumentDataTypes;
	}

	public DataType getAccumulatorDataType() {
		return getOutputDataType();
	}

	public DataType getOutputDataType() {
		Preconditions.checkNotNull(outputDataType, "Output data type not set.");
		return outputDataType;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInference.newBuilder()
			.typedArguments(getArgumentDataTypes())
			.accumulatorTypeStrategy(TypeStrategies.explicit(getAccumulatorDataType()))
			.outputTypeStrategy(TypeStrategies.explicit(getOutputDataType()))
			.build();
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		// in case the function is used for testing
		if (definition != null) {
			definition.getRequirements();
		}
		return super.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		// in case the function is used for testing
		if (definition != null) {
			definition.getRequirements();
		}
		return super.isDeterministic();
	}
}
