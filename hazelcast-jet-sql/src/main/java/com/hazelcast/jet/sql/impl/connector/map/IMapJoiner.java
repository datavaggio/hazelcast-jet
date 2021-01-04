/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.SubDag;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.extract.QueryPath;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.Edge.between;

final class IMapJoiner {

    private IMapJoiner() {
    }

    static SubDag join(
            DAG dag,
            String mapName,
            String tableName,
            JetJoinInfo joinInfo,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        int leftEquiJoinPrimitiveKeyIndex = leftEquiJoinPrimitiveKeyIndex(joinInfo, rightRowProjectorSupplier.paths());
        if (leftEquiJoinPrimitiveKeyIndex > -1) {
            return new SubDag(
                    dag.newUniqueVertex(
                            "Join(Lookup-" + tableName + ")",
                            new JoinByPrimitiveKeyProcessorSupplier(
                                    joinInfo.isInner(),
                                    leftEquiJoinPrimitiveKeyIndex,
                                    joinInfo.condition(),
                                    mapName,
                                    rightRowProjectorSupplier
                            )
                    ),
                    edge -> edge.partitioned(extractPrimitiveKeyFn(leftEquiJoinPrimitiveKeyIndex)).distributed()
            );
        } else if (joinInfo.isEquiJoin() && joinInfo.isInner()) {
            // TODO: define new edge type (mix of broadcast & local-round-robin) ?
            Vertex ingress = dag
                    .newUniqueVertex("Broadcast", () -> new TransformP<>(Traversers::singleton))
                    .localParallelism(1);

            Vertex egress = dag.newUniqueVertex(
                    "Join(Predicate-" + tableName + ")",
                    JoinByPredicateInnerProcessorSupplier.supplier(joinInfo, mapName, rightRowProjectorSupplier)
            );

            dag.edge(between(ingress, egress).partitioned(wholeItem()));

            return new SubDag(ingress, egress, edge -> edge.distributed().broadcast());
        } else if (joinInfo.isEquiJoin() && joinInfo.isLeftOuter()) {
            return new SubDag(
                    dag.newUniqueVertex(
                            "Join(Predicate-" + tableName + ")",
                            new JoinByPredicateOuterProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        } else {
            return new SubDag(
                    dag.newUniqueVertex(
                            "Join(Scan-" + tableName + ")",
                            new JoinScanProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        }
        // TODO: detect and handle always-false condition ?
    }

    /**
     * Find the index of the field of the left side of a join that is in equals
     * predicate with the entire right-side key. Returns -1 if there's no
     * equi-join condition or the equi-join doesn't compare the key object of
     * the right side.
     * <p>
     * For example, in:
     * <pre>{@code
     *     SELECT *
     *     FROM l
     *     JOIN r ON l.field1=right.__key
     * }</pre>
     * it will return the index of {@code field1} in the left table.
     */
    private static int leftEquiJoinPrimitiveKeyIndex(JetJoinInfo joinInfo, QueryPath[] rightPaths) {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int i = 0; i < rightEquiJoinIndices.length; i++) {
            QueryPath path = rightPaths[rightEquiJoinIndices[i]];
            if (path.isTop() && path.isKey()) {
                return joinInfo.leftEquiJoinIndices()[i];
            }
        }
        return -1;
    }

    private static FunctionEx<Object, ?> extractPrimitiveKeyFn(int index) {
        return row -> {
            Object value = ((Object[]) row)[index];
            return value == null ? "" : value;
        };
    }
}
