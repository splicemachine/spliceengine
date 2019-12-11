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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import org.spark_project.guava.base.Joiner;
import org.spark_project.guava.collect.Lists;

import java.util.List;

/**
 * An HalfOuterJoinNode represents a left or a right outer join result set.
 * Right outer joins are always transformed into left outer joins during
 * preprocessing for simplicity.
 */

public class HalfOuterJoinNode extends JoinNode{
    private boolean rightOuterJoin;
    private boolean transformed=false;

    /**
     * Initializer for a HalfOuterJoinNode.
     *
     * @param leftResult      The ResultSetNode on the left side of this join
     * @param rightResult     The ResultSetNode on the right side of this join
     * @param onClause        The ON clause
     * @param usingClause     The USING clause
     * @param rightOuterJoin  Whether or not this node represents a user
     *                        specified right outer join
     * @param tableProperties Properties list associated with the table
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(
            Object leftResult,
            Object rightResult,
            Object onClause,
            Object usingClause,
            Object rightOuterJoin,
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
        this.rightOuterJoin=(Boolean)rightOuterJoin;

		/* We can only flatten an outer join
         * using the null intolerant predicate xform.
		 * In that case, we will return an InnerJoin.
		 */
        flattenableJoin=false;
    }

	/*
	 *  Optimizable interface
	 */

    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        /* We should never push the predicate to joinPredicates as in JoinNode.  joinPredicates
		 * should only be predicates relating the two joining tables.  In the case of half join,
		 * it is biased.  If the general predicate (not join predicate) contains refernce to right
		 * result set, and if doesn't qualify, we shouldn't return the row for the result to be
		 * correct, but half join will fill right side NULL and return the row.  So we can only
		 * push predicate to the left, as we do in "pushExpression".  bug 5055
		 */
        FromTable leftFromTable=(FromTable)leftResultSet;
        return leftFromTable.getReferencedTableMap().contains(optimizablePredicate.getReferencedMap())
                && leftFromTable.pushOptPredicate(optimizablePredicate);
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            return "rightOuterJoin: "+rightOuterJoin+"\n"+
                    "transformed: "+transformed+"\n"+
                    super.toString();
        }else{
            return "";
        }
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

		/* Transform right outer joins to the equivalent left outer join */
        if(rightOuterJoin){
			/* Verify that a user specifed right outer join is transformed into
			 * a left outer join exactly once.
			 */
            assert !transformed: "Attempting to transform a right outer join multiple times";

            ResultSetNode tmp=leftResultSet;

            leftResultSet=rightResultSet;
            rightResultSet=tmp;
            transformed=true;
        }

        newTreeTop=super.preprocess(numTables,gbl,fromList);

        return newTreeTop;
    }

    /**
     * Push expressions down to the first ResultSetNode which can do expression
     * evaluation and has the same referenced table map.
     * RESOLVE - This means only pushing down single table expressions to
     * DistinctNodes today.  Once we have a better understanding of how
     * the optimizer will work, we can push down join clauses.
     *
     * @param outerPredicateList The PredicateList from the outer RS.
     * @throws StandardException Thrown on error
     */
    @Override
    public void pushExpressions(PredicateList outerPredicateList) throws StandardException{
        FromTable leftFromTable=(FromTable)leftResultSet;
        FromTable rightFromTable=(FromTable)rightResultSet;

		/* We only try to push single table predicates to the left.
		 * Pushing them to the right would give incorrect semantics.
		 * We use the logic for pushing down single table predicates here.
		 */
        pushExpressionsToLeft(outerPredicateList);

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
                                     "HalfOuterJoinNode pushing predicate right.",predicate);
            getRightPredicateList().addPredicate(predicate);

			/* Remove the matching predicate from the outer list */
            joinPredicates.removeElementAt(index);
        }

		/* Recurse down both sides of tree */
        PredicateList noPredicates= (PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());
        leftFromTable.pushExpressions(getLeftPredicateList());
        rightFromTable.pushExpressions(noPredicates);
    }

    /**
     * This method recursively:
     * <ul>
     * <li>determines if this part of the query tree is a compound OJ of
     * the shape required for reordering and if so,</li>
     * <li>does a reordering.</li>
     * </ul>
     * <pre>
     *
     *    OJ1  pT1T2                      OJ1  pT2T3
     *   /  \                             / \
     *  /    \                 can       /   t3
     * t1    OJ2 pT2T3       reorder    /
     *       /  \              to      OJ2  pT1T2
     *      /    \                    /   \
     *     t2    t3                  /     \
     *                             t1     t2
     *
     * where pR1R2 is a null-rejecting predicate which references the schema
     * of joinee R1 and R2, cf. terminology explanation in #isNullRejecting.
     *
     * OJ1 represents <em>this</em> before and after the reordering.
     * </pre>
     * <p/>
     * The join predicates are assumed to be in CNF form.
     * <p/>
     * <em>Note:</em> Present implementation limitations
     * <ul>
     * <li>Only left outer joins are considered, i.e. both OJs in diagram
     * above must be LOJ.</li>
     * <li>Top left side must be a base table (t1 above). The bottow right
     * side
     * (t3 above) may be another OJ, so reordering can happen
     * recursively.</li>
     * </ul>
     *
     * @param numTables number of tables involved (needed to right size the
     *                  bitmaps)
     * @return boolean true if any reordering took place at this level or deeper
     * so caller can know whether rebinding may be necessary
     * @throws StandardException standard error policy
     */
    @Override
    public boolean LOJ_reorderable(int numTables) throws StandardException{
        boolean anyChange=false;

        ResultSetNode logicalLeftResultSet;  // row-preserving side
        ResultSetNode logicalRightResultSet; // null-producing side

        // Figure out which is the row-preserving side and which is
        // null-producing side.
        if(rightOuterJoin){ // right outer join
            logicalLeftResultSet=rightResultSet;
            logicalRightResultSet=leftResultSet;
        }else{
            logicalLeftResultSet=leftResultSet;
            logicalRightResultSet=rightResultSet;
        }

        // Redundantly normalize the ON predicate (it will also be called in preprocess()).
        super.normExpressions();

        // This is a very simple OJ of base tables. Do nothing.
        if(logicalLeftResultSet instanceof FromBaseTable && logicalRightResultSet instanceof FromBaseTable)
            return false;

        // Recursively check if we can reordering OJ, and build the table
        // references. Note that joins may have been reordered and therefore the
        // table references need to be recomputed.
        if(logicalLeftResultSet instanceof HalfOuterJoinNode){
            anyChange=((HalfOuterJoinNode)logicalLeftResultSet).LOJ_reorderable(numTables);
        }else if(!(logicalLeftResultSet instanceof FromBaseTable)){// left operand must be either a base table or another OJ
            // In principle, we don't care about the left operand.  However, we
            // need to re-bind the resultColumns.  If the left operand is a
            // view, we may have to re-bind the where clause etc...
            // We ran into difficulty for the following query:
            //  create view v8 (cv, bv, av) as (select c, b, a from t union select f, e, d from s);
            //  select * from v8 left outer join (s left outer join r on (f = i)) on (e=v8.bv);
            return false;
        }

        if(logicalRightResultSet instanceof HalfOuterJoinNode){
            anyChange=((HalfOuterJoinNode)logicalRightResultSet).LOJ_reorderable(numTables) || anyChange;
        }else if(!(logicalRightResultSet instanceof FromBaseTable)){// right operand must be either a base table or another OJ
            return anyChange;
        }

        // It is much easier to do OJ reordering if there is no ROJ.
        // However, we ran into some problem downstream when we transform an ROJ
        // into LOJ -- transformOuterJoin() didn't expect ROJ to be transformed
        // into LOJ alread.  So, we skip optimizing ROJ at the moment.
        if(rightOuterJoin ||
                (logicalRightResultSet instanceof HalfOuterJoinNode
                        && ((HalfOuterJoinNode)logicalRightResultSet).rightOuterJoin)){
            return LOJ_bindResultColumns(anyChange);
        }

        // Build the data structure for testing/doing OJ reordering.  Fill in
        // the table references on row-preserving and null-producing sides.  It
        // may be possible that either operand is a complex view.

        JBitSet RPReferencedTableMap; // Row-preserving
        JBitSet NPReferencedTableMap; // Null-producing

        RPReferencedTableMap=logicalLeftResultSet.LOJgetReferencedTables(numTables);
        NPReferencedTableMap=logicalRightResultSet.LOJgetReferencedTables(numTables);

        if((RPReferencedTableMap==null || NPReferencedTableMap==null) &&
                anyChange){
            return LOJ_bindResultColumns(true);
        }


        // Check if logical right operand is another OJ... so we may be able
        // to push the join.
        if(logicalRightResultSet instanceof HalfOuterJoinNode){
            // Get the row-preserving map of the  child OJ
            JBitSet nestedChildOJRPRefTableMap=((HalfOuterJoinNode)logicalRightResultSet).LOJgetRPReferencedTables(numTables);

            // Checks that top has p(t1,t2)
            if(!isNullRejecting( joinClause, RPReferencedTableMap, nestedChildOJRPRefTableMap)){
                // No, give up.
                return LOJ_bindResultColumns(anyChange);
            }

            // Get the null-producing map of the child OJ
            JBitSet nestedChildOJNPRefTableMap=((HalfOuterJoinNode)logicalRightResultSet).LOJgetNPReferencedTables(numTables);

            // Checks that right child has p(t2,t3)
            if(isNullRejecting(((HalfOuterJoinNode)logicalRightResultSet).joinClause,
                    nestedChildOJRPRefTableMap,
                    nestedChildOJNPRefTableMap)){
                // Push the current OJ into the next level For safety, check
                // the JoinNode data members: they should null or empty list
                // before we proceed.
                if(!super.subqueryList.isEmpty()
                        || !((JoinNode) logicalRightResultSet).subqueryList.isEmpty()
                        || !super.joinPredicates.isEmpty()
                        || !((JoinNode) logicalRightResultSet).joinPredicates.isEmpty()
                        || super.usingClause!=null
                        || ((JoinNode)logicalRightResultSet).usingClause!=null){

                    return LOJ_bindResultColumns(anyChange); //  get out of here
                }
                anyChange=true; // we are reordering the OJs.

                ResultSetNode LChild, RChild;

                //            this OJ
                //            /      \
                //  logicalLeftRS   LogicalRightRS
                //                   /     \
                //                LChild  RChild
                // becomes
                //
                //               this OJ
                //               /      \
                //     LogicalRightRS   RChild
                //           /     \
                // logicalLeftRS LChild <<< we need to be careful about this
                //                          order as the "LogicalRightRS
                //                          may be a ROJ
                //

                // handle the lower level OJ node
                LChild=((HalfOuterJoinNode)logicalRightResultSet).leftResultSet;
                RChild=((HalfOuterJoinNode)logicalRightResultSet).rightResultSet;

                ((HalfOuterJoinNode)logicalRightResultSet).rightResultSet=LChild;
                ((HalfOuterJoinNode)logicalRightResultSet).leftResultSet=logicalLeftResultSet;

                // switch the ON clause
                {
                    ValueNode vn=joinClause;
                    joinClause=((HalfOuterJoinNode)logicalRightResultSet).joinClause;
                    ((HalfOuterJoinNode)logicalRightResultSet).joinClause=vn;
                }

                // No need to switch HalfOuterJoinNode data members for now
                // because we are handling only OJ.
                // boolean local_rightOuterJoin = rightOuterJoin;
                // boolean local_transformed    = transformed;
                // rightOuterJoin = ((HalfOuterJoinNode)logicalRightResultSet).
                //     rightOuterJoin;
                // transformed = ((HalfOuterJoinNode)logicalRightResultSet).
                //     transformed;
                // ((HalfOuterJoinNode)logicalRightResultSet).rightOuterJoin =
                //     local_rightOuterJoin;
                // ((HalfOuterJoinNode)logicalRightResultSet).transformed =
                //     local_transformed;

                FromList localFromList=(FromList)getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager());

                // switch OJ nodes: by handling the current OJ node
                leftResultSet=logicalRightResultSet;
                rightResultSet=RChild;

                // rebuild the result columns and re-bind column references
                ((HalfOuterJoinNode)leftResultSet).resultColumns=null;
                // localFromList is empty:
                leftResultSet.bindResultColumns(localFromList);

                // left operand must be another OJ, so recurse.
                ((HalfOuterJoinNode)leftResultSet).LOJ_reorderable(numTables);
            }
        }

        return LOJ_bindResultColumns(anyChange);
    }


    /**
     * Tests pRiRj in the sense of Galindo-Legaria et al: <em>Outerjoin
     * Simplification and Reordering for Query Optimization</em>, ACM
     * Transactions on Database Systems, Vol. 22, No. 1, March 1997, Pages
     * 43-74:
     * <quote>
     * "The set of attributes referenced by a predicate p is called the schema
     * of p, and denoted sch(p). As a notational convention, we annotate
     * predicates to reflect their schema. If sch(p) includes attributes of
     * both Ri, Rj and only those relations, we can write the predicate as
     * pRiRj.
     * </quote>
     * <p/>
     * If a null-valued column is compared in a predicate that
     * contains no OR connectives, the predicate evaluates to undefined, and
     * the tuple is rejected. The relops satisfy this criterion.
     * <p/>
     * To simplify analysis, we only accept predicates of the form:
     * <pre>
     * X relop Y [and .. and X-n relop Y-n]
     * </pre>
     * <p/>
     * At least one of the relops should reference both {@code leftTableMap}
     * and {@code rightTableMap}, so that we know that sch(p) includes
     * attributes of both Ri, Rj. I.e.
     * <p/>
     * <p/>
     * {@code X} should be a table in {@code leftTableMap}, and
     * {@code Y} should be a table in {@code rightTableMap}.
     * <p/>
     * <b>or</b>
     * {@code X} should be a table in {@code rightTableMap}, and
     * {@code Y} should be a table in {@code leftTableMap}.
     *
     * @param joinClause    The join clause (i.e. predicate) we want to check
     * @param leftTableMap  a bit map representing the tables expected for the
     *                      predicate (logical left)
     * @param rightTableMap a bit map representing the tables expected for the
     *                      predicate (logical right)
     * @return true if the {@code joinClause} has at least one relop that
     * references both {@code leftTableMap} and {@code
     * rightTableMap}
     * @throws StandardException standard exception policy
     */

    private boolean isNullRejecting(ValueNode joinClause,
                                    JBitSet leftTableMap,
                                    JBitSet rightTableMap) throws StandardException{
        ValueNode vn=joinClause;
        boolean foundPred=false;

        while(vn!=null){
            AndNode andNode=null;

            if(vn instanceof AndNode){
                andNode=(AndNode)vn;
                vn=andNode.getLeftOperand();
            }

            if(vn instanceof BinaryRelationalOperatorNode){

                BinaryRelationalOperatorNode relop= (BinaryRelationalOperatorNode)vn;
                ValueNode leftCol=relop.getLeftOperand();
                ValueNode rightCol=relop.getRightOperand();

                boolean leftFound=false;
                boolean rightFound=false;

                if(leftCol instanceof ColumnReference){
                    if(leftTableMap.get(((ColumnReference)leftCol).getTableNumber())){
                        leftFound=true;
                    }else if(rightTableMap.get(((ColumnReference)leftCol).getTableNumber())){
                        rightFound=true;
                    }else{
                        // references unexpected table
                        return false;
                    }
                }

                if(rightCol instanceof ColumnReference){
                    if(leftTableMap.get(((ColumnReference)rightCol).getTableNumber())){
                        leftFound=true;
                    }else if(rightTableMap.get(((ColumnReference)rightCol).getTableNumber())){
                        rightFound=true;
                    }else{
                        // references unexpected table, sch(p) is wrong
                        return false;
                    }
                }


                if(leftFound && rightFound){
                    foundPred=true; // sch(p) covers both R1 and R2
                }
            }else if((!(vn instanceof BooleanConstantNode)) || !foundPred){
                // reject other operators, e.g. OR
                return false;
            }
            // OK, simple predicate which covers both R1 and R2 found

            if(andNode!=null){
                vn=andNode.getRightOperand();
            }else{
                vn=null;
            }
        }

        return foundPred;
    }


    // This method re-binds the result columns which may be referenced in the ON
    // clause in this node.
    public boolean LOJ_bindResultColumns(boolean anyChange) throws StandardException{
        if(anyChange){
            this.resultColumns=null;
            FromList localFromList=(FromList)getNodeFactory().getNode(C_NodeTypes.FROM_LIST,
                    getNodeFactory().doJoinOrderOptimization(),
                    getContextManager());
            this.bindResultColumns(localFromList);
        }
        return anyChange;
    }


    /**
     * Transform any Outer Join into an Inner Join where applicable.
     * (Based on the existence of a null intolerant
     * predicate on the inner table.)
     *
     * @param predicateTree The predicate tree for the query block
     * @return The new tree top (OuterJoin or InnerJoin).
     * @throws StandardException Thrown on error
     */
    @Override
    public FromTable transformOuterJoins(ValueNode predicateTree,int numTables) throws StandardException{
        ResultSetNode innerRS;

        if(predicateTree==null){
			/* We can't transform this node, so tell both sides of the 
			 * outer join that they can't get flattened into outer query block.
			 */
            leftResultSet.notFlattenableJoin();
            rightResultSet.notFlattenableJoin();
            return this;
        }

        super.transformOuterJoins(predicateTree,numTables);

        if(rightOuterJoin){
            assert !transformed: "right OJ not expected to be transformed into left OJ yet";
            innerRS=leftResultSet;
        }else{
            innerRS=rightResultSet;
        }

        // We are looking for a null intolerant predicate on an inner table.
        // Collect base table numbers, also if they are located inside a join
        // (inner or outer), that is, the inner operand is itself a join,
        // recursively.
        JBitSet innerMap=innerRS.LOJgetReferencedTables(numTables);

		/* Walk predicates looking for 
		 * a null intolerant predicate on the inner table.
		 */
        ValueNode vn=predicateTree;
        while(vn instanceof AndNode){
            AndNode and=(AndNode)vn;
            ValueNode left=and.getLeftOperand();

			/* Skip IS NULL predicates as they are not null intolerant */
            if(left.isInstanceOf(C_NodeTypes.IS_NULL_NODE)){
                vn=and.getRightOperand();
                continue;
            }

			/* Only consider predicates that are relops */
            if(left instanceof RelationalOperator){
                JBitSet refMap=new JBitSet(numTables);
				/* Do not consider method calls, 
				 * conditionals, field references, etc. */
                if(!(left.categorize(refMap,true))){
                    vn=and.getRightOperand();
                    continue;
                }

				/* If the predicate is a null intolerant predicate
				 * on the right side then we can flatten to an
				 * inner join.  We do the xform here, flattening
				 * will happen later.
				 */
                for(int bit=0;bit<numTables;bit++){
                    if(refMap.get(bit) && innerMap.get(bit)){
                        // OJ -> IJ
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
                }
            }

            vn=and.getRightOperand();
        }

		/* We can't transform this node, so tell both sides of the 
		 * outer join that they can't get flattened into outer query block.
		 */
        leftResultSet.notFlattenableJoin();
        rightResultSet.notFlattenableJoin();

        return this;
    }

    @Override
    protected void adjustNumberOfRowsReturned(CostEstimate costEstimate){
		/*
		** An outer join returns at least as many rows as in the outer
		** table. Even if this started as a right outer join, it will
		** have been transformed to a left outer join by this point.
		*/
        CostEstimate outerCost=getLeftResultSet().getCostEstimate();

        if(costEstimate.rowCount()<outerCost.rowCount()){
            costEstimate.setCost(costEstimate.getEstimatedCost(), outerCost.rowCount(), outerCost.rowCount());
        }
    }

    /**
     * Generate the code for an inner join node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
		/* Verify that a user specifed right outer join is transformed into
		 * a left outer join exactly once.
		 */
        assert rightOuterJoin==transformed:
                "rightOuterJoin ("+rightOuterJoin+ ") is expected to equal transformed ("+transformed+")";
        super.generateCore(acb,mb,LEFTOUTERJOIN);
    }

    /**
     * Generate	and add any arguments specifict to outer joins.
     * Generate	the methods (and add them as parameters) for
     * returning an empty row from 1 or more sides of an outer join,
     * if required.  Pass whether or not this was originally a
     * right outer join.
     *
     * @param acb The ActivationClassBuilder
     * @param mb  the method the generate code is to go into
     *            <p/>
     *            return The args that have been added
     * @throws StandardException Thrown on error
     */
    @Override
    protected int addOuterJoinArguments(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
		/* Nulls always generated from the right */
        rightResultSet.getResultColumns().generateNulls(acb,mb);

		/* Was this originally a right outer join? */
        mb.push(rightOuterJoin);

        return 2;
    }

    /**
     * Return the number of arguments to the join result set.
     */
    @Override
    protected int getNumJoinArguments(){
		/* We add two more arguments than the superclass does */
        return super.getNumJoinArguments()+2;
    }

    @Override
    protected void oneRowRightSide(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        // always return false for now
        mb.push(rightResultSet.getFromSSQ() && rightResultSet.isOneRowResultSet());
        mb.push(false);  //isNotExists?
    }

    /**
     * Return the logical left result set for this qualified
     * join node.
     * (For RIGHT OUTER JOIN, the left is the right
     * and the right is the left and the JOIN is the NIOJ).
     */
    @Override
    ResultSetNode getLogicalLeftResultSet(){
        if(rightOuterJoin){
            return rightResultSet;
        }else{
            return leftResultSet;
        }
    }

    /**
     * Return the logical right result set for this qualified
     * join node.
     * (For RIGHT OUTER JOIN, the left is the right
     * and the right is the left and the JOIN is the NIOJ).
     */
    @Override
    ResultSetNode getLogicalRightResultSet(){
        if(rightOuterJoin){
            return leftResultSet;
        }else{
            return rightResultSet;
        }
    }

    /**
     * Return true if right outer join or false if left outer join
     * Used to set Nullability correctly in JoinNode
     */
    public boolean isRightOuterJoin(){
        return rightOuterJoin;
    }

    /**
     * If this is a right outer join node with USING/NATURAL clause, then
     * check if the passed ResultColumn is a join column. If yes, then
     * ResultColumn should be marked such. DERBY-4631
     */
    @Override
    public void isJoinColumnForRightOuterJoin(ResultColumn rc){
        if(isRightOuterJoin() && usingClause!=null &&usingClause.getResultColumn(rc.getUnderlyingOrAliasName())!=null){
            rc.setRightOuterJoinUsingClause(true);
            rc.setJoinResultset(this);
        }
    }

    // return the Null-producing table references
    public JBitSet LOJgetNPReferencedTables(int numTables)
            throws StandardException{
        if(rightOuterJoin && !transformed)
            return leftResultSet.LOJgetReferencedTables(numTables);
        else
            return rightResultSet.LOJgetReferencedTables(numTables);
    }

    // return the row-preserving table references
    public JBitSet LOJgetRPReferencedTables(int numTables)
            throws StandardException{
        if(rightOuterJoin && !transformed)
            return rightResultSet.LOJgetReferencedTables(numTables);
        else
            return leftResultSet.LOJgetReferencedTables(numTables);
    }

    public boolean isUnsatisfiable() {
        if (sat == Satisfiability.UNSAT)
            return true;

        if (sat != Satisfiability.UNKNOWN)
            return false;

        if (leftResultSet.isUnsatisfiable()) {
            sat = Satisfiability.UNSAT;
            return true;
        }

        sat = Satisfiability.NEITHER;
        return false;
    }

    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        JoinStrategy joinStrategy = RSUtils.ap(this).getJoinStrategy();
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append(joinStrategy.getJoinStrategyType().niceName()).append(isRightOuterJoin()?"RightOuter":"LeftOuter").append("Join(")
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
    public String toHTMLString() {
        return "rightOuterJoin: " + rightOuterJoin + "<br/>" +
                "transformed: " + transformed + "<br/>" +
                super.toHTMLString();
    }

}
