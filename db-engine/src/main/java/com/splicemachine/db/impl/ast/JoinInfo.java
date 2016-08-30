/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.ast;

import org.spark_project.guava.base.Function;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.impl.sql.compile.*;
import org.spark_project.guava.collect.Iterables;

import java.util.List;
import static java.lang.String.format;

/**
 * @author P Trolard
 *         Date: 17/09/2013
 */
public class JoinInfo {
    public final JoinStrategy strategy;
    public final boolean userSuppliedStrategy;
    public final boolean isSystemTable;
    public final boolean isEquiJoin;
    public final boolean hasRightIndex;
    public final List<Predicate> joinPredicates;
    public final List<Predicate> otherPredicates;
    public final List<ResultSetNode> rightNodes;
    public final List<ResultSetNode> rightLeaves;
    public final int rightSingleRegionSize;

    public JoinInfo(JoinStrategy strategy,
                    boolean userSuppliedStrategy, boolean isSystemTable,
                    boolean isEquiJoin, boolean hasRightIndex,
                    List<Predicate> joinPredicates, List<Predicate> otherPredicates,
                    List<ResultSetNode> rightNodes, List<ResultSetNode> rightLeaves,
                    int rightSingleRegionSize){
        this.strategy = strategy;
        this.userSuppliedStrategy = userSuppliedStrategy;
        this.isSystemTable = isSystemTable;
        this.isEquiJoin = isEquiJoin;
        this.hasRightIndex = hasRightIndex;
        this.joinPredicates = joinPredicates;
        this.otherPredicates = otherPredicates;
        this.rightNodes = rightNodes;
        this.rightLeaves = rightLeaves;
        this.rightSingleRegionSize = rightSingleRegionSize;
    }


    public String toString(){
        return format("{" +
                          "strategy=%s, " +
                          "userSuppliedStrategy=%s, " +
                          "isSystemTable=%s, " +
                          "isEquijoin=%s, " +
                          "hasRightIndex=%s, " +
                          "joinPreds=%s, " +
                          "otherPreds=%s, " +
                          "rightNodes=%s, " +
                          "rightLeaves=%s, " +
                          "rightSingleRegionSize=%s",
                         strategy, userSuppliedStrategy, isSystemTable,
                         isEquiJoin, hasRightIndex,
                         Iterables.transform(joinPredicates, PredicateUtils.predToString),
                         Iterables.transform(otherPredicates, PredicateUtils.predToString),
                         Iterables.transform(rightNodes, className),
                         Iterables.transform(rightLeaves, className),
                         rightSingleRegionSize);
    }

    public static Function<Object, String> className = new Function<Object, String>() {
        @Override
        public String apply(Object o) {
            return o == null ? "" : o.getClass().getSimpleName();
        }
    };

}
