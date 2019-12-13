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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.iapi.sql.compile.OptimizerFlag;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import org.spark_project.guava.base.Joiner;
import org.spark_project.guava.collect.Lists;

import java.util.List;

/**
 * Created by yxia on 11/26/19.
 */
public class FullOuterJoinNode extends JoinNode {
    /**
     * Initializer for a FullOuterJoinNode.
     *
     * @param leftResult      The ResultSetNode on the left side of this join
     * @param rightResult     The ResultSetNode on the right side of this join
     * @param onClause        The ON clause
     * @param usingClause     The USING clause
     * @param tableProperties Properties list associated with the table
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(
            Object leftResult,
            Object rightResult,
            Object onClause,
            Object usingClause,
            Object tableProperties)
            throws StandardException{
        super.init(
                leftResult,
                rightResult,
                onClause,
                usingClause,
                null,
                tableProperties,
                null);

		/* We can only flatten an outer join
         * using the null intolerant predicate xform.
		 * In that case, we may return a Left/Right/InnerJoin.
		 */
        flattenableJoin=false;
    }

	/*
	 *  Optimizable interface
	 */

    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        /* we cannot push single table condition to either side of a full join */
        return false;
    }


    /**
     * Put a ProjectRestrictNode on top of each FromTable in the FromList.
     * ColumnReferences must continue to point to the same ResultColumn, so
     * that ResultColumn must percolate up to the new PRN.  However,
     * that ResultColumn will point to a new expression, a VirtualColumnNode,
     * which points to the FromTable and the ResultColumn that is the source for
     * the ColumnReference.
     * (The new PRN will have the original of the ResultColumnList and
     * the ResultColumns from that list.  The FromTable will get shallow copies
     * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
     * will remain at the FromTable, with the PRN getting a new
     * VirtualColumnNode for each ResultColumn.expression.)
     * We then project out the non-referenced columns.  If there are no referenced
     * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
     * whose expression is 1.
     *
     * @param numTables Number of tables in the DML Statement
     * @param gbl       The group by list, if any
     * @param fromList  The from list, if any
     * @return The generated ProjectRestrictNode atop the original FromTable.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode preprocess(int numTables, GroupByList gbl, FromList fromList) throws StandardException{
        ResultSetNode newTreeTop;

        newTreeTop=super.preprocess(numTables,gbl,fromList);

        return newTreeTop;
    }

    /**
     * @param outerPredicateList The PredicateList from the outer RS.
     * @throws StandardException Thrown on error
     */
    @Override
    public void pushExpressions(PredicateList outerPredicateList) throws StandardException{
        FromTable leftFromTable=(FromTable)leftResultSet;
        FromTable rightFromTable=(FromTable)rightResultSet;

		/* Push the pushable outer join predicates to the right.  This is done
		 * bottom up, hence at the end of this method, so that outer join
		 * conditions only get pushed down 1 level.
		 * We use the optimizer's logic for pushing down join clause here.
		 */
        // Walk joinPredicates backwards due to possible deletes
        for(int index=joinPredicates.size()-1;index>=0;index--){
            Predicate predicate;

            predicate=joinPredicates.elementAt(index);
            if(!predicate.getPushable()){
                continue;
            }

            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION,0,0,0.0,
                    "FullOuterJoinNode pushing predicate right.",predicate);
            getRightPredicateList().addPredicate(predicate);

			/* Remove the matching predicate from the outer list */
            joinPredicates.removeElementAt(index);
        }

		/* Recurse down both sides of tree */
        PredicateList noPredicates= (PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());
        leftFromTable.pushExpressions(noPredicates);
        rightFromTable.pushExpressions(noPredicates);
    }

    /**
     * Generate the code for an full outer join node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        super.generateCore(acb,mb,FULLOUTERJOIN);
    }

    @Override
    protected int addOuterJoinArguments(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
		/* Nulls from the left */
        leftResultSet.getResultColumns().generateNulls(acb,mb);

        /* Nulls from the right */
        rightResultSet.getResultColumns().generateNulls(acb,mb);

		/* wasRightOuterJoin */
        mb.push(false);

        return 3;
    }

    /**
     * Return the number of arguments to the join result set.
     */
    @Override
    protected int getNumJoinArguments(){
		/* We add two more arguments than the superclass does */
        return super.getNumJoinArguments()+3;
    }

    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        JoinStrategy joinStrategy = RSUtils.ap(this).getJoinStrategy();
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append(joinStrategy.getJoinStrategyType().niceName()).append("FullOuter").append("Join(")
                .append("n=").append(order)
                .append(attrDelim).append(getFinalCostEstimate(false).prettyProcessingString(attrDelim));
        if (joinPredicates !=null) {
            List<String> joinPreds = Lists.transform(PredicateUtils.PLtoList(joinPredicates), PredicateUtils.predToString);
            if (!joinPreds.isEmpty()) {
                sb.append(attrDelim).append("preds=[").append(Joiner.on(",").skipNulls().join(joinPreds)).append("]");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public FromTable transformOuterJoins(ValueNode predicateTree,int numTables) throws StandardException{

        super.transformOuterJoins(null,numTables);
        if(PredicateSimplificationVisitor.isBooleanTrue(joinClause)){
            JoinNode ij=(JoinNode)
                    getNodeFactory().getNode(
                            C_NodeTypes.JOIN_NODE,
                            leftResultSet,
                            rightResultSet,
                            joinClause,
                            null,
                            resultColumns,
                            null,
                            null,
                            getContextManager());
            ij.setTableNumber(tableNumber);
            ij.setSubqueryList(subqueryList);
            ij.setAggregateVector(aggregateVector);
            
            return ij;
        }

		/* We can't transform this node, so tell both sides of the
		 * outer join that they can't get flattened into outer query block.
		 */
        leftResultSet.notFlattenableJoin();
        rightResultSet.notFlattenableJoin();

        return this;
    }
}
