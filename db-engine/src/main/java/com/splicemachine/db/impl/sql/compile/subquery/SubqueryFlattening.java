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

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.sql.compile.SelectNode;
import com.splicemachine.db.impl.sql.compile.StatementNode;
import com.splicemachine.db.impl.sql.compile.subquery.aggregate.AggregateSubqueryFlatteningVisitor;
import com.splicemachine.db.impl.sql.compile.subquery.exists.ExistsSubqueryFlatteningVisitor;
import org.sparkproject.guava.collect.Lists;
import org.sparkproject.guava.collect.Multimap;
import org.sparkproject.guava.collect.Multimaps;

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
