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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.commons.lang3.tuple.Pair;
import org.spark_project.guava.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: pjt
 * Date: 7/29/13
 */
public class ColumnUtils {

    public static class IsCorrelatedPredicate implements org.spark_project.guava.base.Predicate<ColumnReference> {

        public static final IsCorrelatedPredicate INSTANCE = new IsCorrelatedPredicate();

        @Override
        public boolean apply(ColumnReference columnReference) {
            return columnReference.getCorrelated();
        }
    }

    /**
     * For a given ResultColumnList, return a map from
     * [resultSetNumber, virtualColumnId] => ResultColumn
     * where there is one entry for each ResultColumn down the chain of reference to
     * its source column on a table. This allows translation from a column reference at
     * any node below into the ResultColumn projected from the passed ResultColumnList.
     */
    public static Map<Pair<Integer, Integer>, ResultColumn> rsnChainMap(ResultColumnList rcl)
            throws StandardException {
        Map<Pair<Integer, Integer>, ResultColumn> chain = new HashMap<Pair<Integer, Integer>, ResultColumn>();
        List<ResultColumn> cols = RSUtils.collectNodes(rcl, ResultColumn.class);

        for (ResultColumn rc : cols) {
            Pair<Integer, Integer> top = RSCoordinate(rc);
            chain.put(top, rc);
            for (Pair<Integer, Integer> link : rsnChain(rc)) {
                chain.put(link, rc);
            }
        }

        return chain;
    }

    /**
     * For a given ResultColumn, return a list of integer pairs [resultSetNumber, virtualColumnId]
     * for its source column, and its source's source column, and so on down to the bottom: a source
     * column on a table.
     */
    public static List<Pair<Integer, Integer>> rsnChain(ResultColumn rc)
            throws StandardException {
        List<Pair<Integer, Integer>> chain = new ArrayList<Pair<Integer, Integer>>();

        ValueNode expression = rc.getExpression();
        while (expression != null) {
            if (expression instanceof VirtualColumnNode) {
                ResultColumn sc = ((VirtualColumnNode) expression).getSourceColumn();
                chain.add(RSCoordinate(sc));
                expression = sc.getExpression();
            } else if (expression instanceof ColumnReference) {
                ResultColumn sc = ((ColumnReference) expression).getSource();
                if (sc != null) { // A ColumnReference can be sourceless…
                    chain.add(RSCoordinate(sc));
                    expression = sc.getExpression();
                } else {
                    expression = null;
                }
            } else if (expression instanceof CastNode) {
                expression = ((CastNode) expression).getCastOperand();
            }
            else {
                expression = null;
            }
        }

        return chain;
    }

    static Pair<Integer, Integer> RSCoordinate(ResultColumn rc) {

        ResultColumn resultColumn = rc;
        ValueNode vn = rc.getExpression();
        if (vn instanceof CastNode) {
            ValueNode castOperand=((CastNode)vn).getCastOperand();
            if(castOperand instanceof VirtualColumnNode){
                VirtualColumnNode vcn=(VirtualColumnNode)castOperand;
                resultColumn=vcn.getSourceColumn();
            }else if(castOperand instanceof ColumnReference){
                ColumnReference cr = (ColumnReference)castOperand;
                resultColumn = cr.getSourceResultColumn();
            }
        }
        return Pair.of(resultColumn.getResultSetNumber(), resultColumn.getVirtualColumnId());
    }

    /**
     * TRUE if the node parameter or any of its descendants are a correlated ColumnReference.
     */
    public static boolean isSubtreeCorrelated(Visitable node) throws StandardException {
        List<ColumnReference> columnReferences = CollectingVisitorBuilder.forClass(ColumnReference.class).collect(node);
        return Iterables.any(columnReferences, IsCorrelatedPredicate.INSTANCE);
    }

}
