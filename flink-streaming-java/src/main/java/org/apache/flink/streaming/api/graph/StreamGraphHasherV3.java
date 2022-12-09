/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;

import org.apache.flink.shaded.guava30.com.google.common.hash.Hasher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.util.StringUtils.byteToHexString;

/**
 * StreamGraphHasher that behaves like {@link StreamGraphHasherV2}, except that it does not include
 * chained nodes into the hash.
 */
public class StreamGraphHasherV3 extends StreamGraphHasherV2 {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraphHasherV3.class);

    /** Generates a deterministic hash from node-local properties and input and output edges. */
    @Override
    protected byte[] generateDeterministicHash(
            StreamNode node,
            Hasher hasher,
            Map<Integer, byte[]> hashes,
            boolean isChainingEnabled,
            StreamGraph streamGraph) {

        // Include stream node to hash. We use the current size of the computed
        // hashes as the ID. We cannot use the node's ID, because it is
        // assigned from a static counter. This will result in two identical
        // programs having different hashes.
        generateNodeLocalHash(hasher, hashes.size());

        byte[] hash = hasher.hash().asBytes();

        // Make sure that all input nodes have their hash set before entering
        // this loop (calling this method).
        for (StreamEdge inEdge : node.getInEdges()) {
            byte[] otherHash = hashes.get(inEdge.getSourceId());

            // Sanity check
            if (otherHash == null) {
                throw new IllegalStateException(
                        "Missing hash for input node "
                                + streamGraph.getSourceVertex(inEdge)
                                + ". Cannot generate hash for "
                                + node
                                + ".");
            }

            for (int j = 0; j < hash.length; j++) {
                hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
            }
        }

        if (LOG.isDebugEnabled()) {
            String udfClassName = "";
            if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                udfClassName =
                        ((UdfStreamOperatorFactory<?>) node.getOperatorFactory())
                                .getUserFunctionClassName();
            }

            LOG.debug(
                    "Generated hash '"
                            + byteToHexString(hash)
                            + "' for node "
                            + "'"
                            + node
                            + "' {id: "
                            + node.getId()
                            + ", "
                            + "parallelism: "
                            + node.getParallelism()
                            + ", "
                            + "user function: "
                            + udfClassName
                            + "}");
        }

        return hash;
    }
}
