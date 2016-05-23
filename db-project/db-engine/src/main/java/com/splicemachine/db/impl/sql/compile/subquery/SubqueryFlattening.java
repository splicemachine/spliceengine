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
