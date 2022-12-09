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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;

import java.util.Optional;

public class ExecNodeUtil {

    /**
     * Sets {Transformation#declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase, int)}
     * using the given bytes for {@link ManagedMemoryUseCase#OPERATOR}.
     */
    public static <T> void setManagedMemoryWeight(
            Transformation<T> transformation, long memoryBytes) {
        if (memoryBytes > 0) {
            final int weightInMebibyte = Math.max(1, (int) (memoryBytes >> 20));
            final Optional<Integer> previousWeight =
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, weightInMebibyte);
            if (previousWeight.isPresent()) {
                throw new TableException(
                        "Managed memory weight has been set, this should not happen.");
            }
        }
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String operatorType,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism) {
        return createOneInputTransformation(
                input, name, operatorType, operator, outputType, parallelism, 0);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String operatorType,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        return createOneInputTransformation(
                input,
                name,
				operatorType,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes);
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String operatorType,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism) {
        return createOneInputTransformation(
                input, name, operatorType, operatorFactory, outputType, parallelism, 0);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String operatorType,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        OneInputTransformation<I, O> transformation =
                new OneInputTransformation<>(
                        input,
                        name,
						operatorType,
                        operatorFactory,
                        outputType,
                        parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        return transformation;
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            String operatorType,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism) {
        return createTwoInputTransformation(
                input1, input2, name, operatorType, operator, outputType, parallelism, 0);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            String operatorType,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        return createTwoInputTransformation(
                input1,
                input2,
                name,
				operatorType,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <I1, I2, O> TwoInputTransformation<I1, I2, O> createTwoInputTransformation(
            Transformation<I1> input1,
            Transformation<I2> input2,
            String name,
            String operatorType,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        TwoInputTransformation<I1, I2, O> transformation =
                new TwoInputTransformation<>(
                        input1,
                        input2,
                        name,
						operatorType,
                        operatorFactory,
                        outputType,
                        parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        return transformation;
    }
}
