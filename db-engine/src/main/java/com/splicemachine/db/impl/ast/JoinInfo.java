/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import splice.com.google.common.base.Function;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.impl.sql.compile.*;
import splice.com.google.common.collect.Iterables;

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

    public static final Function<Object, String> className = new Function<Object, String>() {
        @Override
        public String apply(Object o) {
            return o == null ? "" : o.getClass().getSimpleName();
        }
    };

}
