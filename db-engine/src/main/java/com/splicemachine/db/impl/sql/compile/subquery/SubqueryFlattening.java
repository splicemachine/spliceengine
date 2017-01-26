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

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.sql.compile.SelectNode;
import com.splicemachine.db.impl.sql.compile.StatementNode;
import com.splicemachine.db.impl.sql.compile.subquery.aggregate.AggregateSubqueryFlatteningVisitor;
import com.splicemachine.db.impl.sql.compile.subquery.exists.ExistsSubqueryFlatteningVisitor;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Multimap;
import org.spark_project.guava.collect.Multimaps;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Entry point for splice enhanced subquery flattening.
 */
public class SubqueryFlattening {

    /**
     * We expect this to be invoked once per statement just before pre-process.  The StatementNode passed in is the top
     * level node, probably a CursorNode.
     */
    public static void flatten(StatementNode statementNode) throws StandardException {

        /*
         * Find all SelectNodes that have subqueries.
         */
        List<SelectNode> selectNodesWithSubquery = CollectingVisitorBuilder.<SelectNode>forPredicate(
                new SelectNode.SelectNodeWithSubqueryPredicate()).collect(statementNode
        );

        /*
         * Transform the SelectNode to a Multimap where the subquery nesting level is the key.
         */
        Multimap<Integer, SelectNode> selectMap = Multimaps.index(selectNodesWithSubquery, new SelectNode.SelectNodeNestingLevelFunction());

        /*
         * Sort the levels in descending order.
         */
        List<Integer> levels = Lists.newArrayList(selectMap.keySet());
        Collections.sort(levels, Collections.reverseOrder());

        /*
         * Iterate over nesting levels in reverse order.
         */
        for (Integer nestingLevel : levels) {

            /*
             * Flatten subqueries at this level.
             */
            Collection<SelectNode> selectNodes = selectMap.get(nestingLevel);
            AggregateSubqueryFlatteningVisitor aggregateFlatteningVisitor = new AggregateSubqueryFlatteningVisitor(nestingLevel);
            ExistsSubqueryFlatteningVisitor existsFlatteningVisitor = new ExistsSubqueryFlatteningVisitor(nestingLevel);

            for (SelectNode selectNode : selectNodes) {
                selectNode.accept(aggregateFlatteningVisitor);
                selectNode.accept(existsFlatteningVisitor);
            }

        }
    }

}
