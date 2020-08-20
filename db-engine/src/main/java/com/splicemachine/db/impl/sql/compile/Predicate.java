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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.BitSet;
import java.util.Hashtable;
import java.util.List;

/**
 * A Predicate represents a top level predicate.
 */

public final class Predicate extends QueryTreeNode implements OptimizablePredicate, Comparable<Predicate>{
    /* Top of the predicate */
    AndNode andNode;
    boolean pushable;
    /* Bit map of referenced tables */
    JBitSet referencedSet;
    /* Join clauses are placed into equivalence classes when applying transitive
     * closure for join clauses.  This is useful for eliminating redundant predicates.
     */
    int equivalenceClass=-1;
    // indexPosition of -1 is used by rowId, so use -2 to indicate that it has been set
    int indexPosition=-2;
    protected boolean startKey;
    protected boolean stopKey;
    protected boolean rowId;
    protected boolean isQualifier;
    // Whether this is full outer join predicate
    protected boolean  forFullJoin;
    // indicate whether the predicate is from the ON cluase of a particular half outer join
    protected int outerJoinLevel;
    private boolean pulled;

    /* Hashtable used for tracking the search clause types that have been
     * pushed through this predicate (if an equijoin) via transitive closure.
     */
    private Hashtable searchClauseHT;

    // Whether or not this predicate has been scoped; see the
    // getPredScopedForResultSet() method of this class for more.
    private boolean scoped;

    public void setPulled(boolean pulled){
        this.pulled=pulled;
    }

    public boolean getPulled(){
        return pulled;
    }

    /**
     * Initializer.
     *
     * @param andNode       The top of the predicate
     * @param referencedSet Bit map of referenced tables
     */

    public void init(Object andNode,Object referencedSet){
        this.andNode=(AndNode)andNode;
        pushable=false;
        this.referencedSet=(JBitSet)referencedSet;
        scoped=false;
    }

	/*
     *  Optimizable interface
	 */

    @Override
    public JBitSet getReferencedMap(){
        return referencedSet;
    }

    @Override
    public boolean hasSubquery(){
        /* RESOLVE - Currently, we record whether or not a predicate is pushable based
		 * on whether or not it contains a subquery or method call, but we do not
		 * record the underlying info.
		 */
        return !pushable;
    }

    @Override
    public boolean hasMethodCall(){
		/* RESOLVE - Currently, we record whether or not a predicate is pushable based
		 * on whether or not it contains a subquery or method call, but we do not
		 * record the underlying info.
		 */
        return !pushable;
    }

    @Override
    public void markStartKey(){
        startKey=true;
    }

    @Override
    public boolean isRowId(){
        return rowId;
    }

    public void markRowId(){
        rowId=true;
    }

    @Override
    public boolean isStartKey(){
        return startKey;
    }

    @Override
    public void markStopKey(){
        stopKey=true;
    }

    @Override
    public boolean isStopKey(){
        return stopKey;
    }

    @Override
    public void markQualifier(){
        isQualifier=true;
    }

    @Override
    public boolean isQualifier(){
        return isQualifier;
    }

    @Override
    public boolean compareWithKnownConstant(Optimizable optTable,boolean considerParameters){
        boolean retval=false;
        RelationalOperator relop=getRelop();

		/* if this is for "in" operator node's dynamic start/stop key, relop is
		 * null, and it's not comparing with constant, beetle 3858
		 */
        if(!isRelationalOpPredicate())
            return false;

        assert relop!=null;
        if(relop.compareWithKnownConstant(optTable,considerParameters))
            retval=true;

        return retval;
    }

    @Override
    public int hasEqualOnColumnList(int[] baseColumnPositions,Optimizable optTable) throws StandardException{
        RelationalOperator relop=getRelop();
        assert relop!=null;

        if(!isRelationalOpPredicate())
            return -1;

        if(!(relop.getOperator()==RelationalOperator.EQUALS_RELOP))
            return -1;

        for(int i=0;i<baseColumnPositions.length;i++){
            ColumnReference cr=relop.getColumnOperand(optTable,baseColumnPositions[i]);

            if(cr==null)
                continue;

            if(relop.selfComparison(cr))
                continue;

            // If I made it thus far in the loop, we've found
            // something.
            return i;
        }

        return -1;
    }

    @Override
    public DataValueDescriptor getCompareValue(Optimizable optTable) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(compareWithKnownConstant(optTable,true),
                    "Cannot get the compare value if not comparing with a known constant.");
        }

        RelationalOperator relop=getRelop();
        assert relop!=null;

        return relop.getCompareValue(optTable);
    }

    @Override
    public boolean equalsComparisonWithConstantExpression(Optimizable optTable){
        boolean retval=false;

        if(isRelationalOpPredicate()){
            RelationalOperator relop=getRelop();
            assert relop!=null;
            retval=relop.equalsComparisonWithConstantExpression(optTable);
        }

        return retval;
    }

    @Override
    public double selectivity(Optimizable optTable) throws StandardException{
        return andNode.getLeftOperand().selectivity(optTable);
    }

    @Override
    public double joinSelectivity(Optimizable table,ConglomerateDescriptor cd, long innerRowCount, long outerRowCount,SelectivityUtil.SelectivityJoinType selectivityJoinType) throws StandardException{
        return andNode.getLeftOperand().joinSelectivity(table,cd,innerRowCount,outerRowCount,selectivityJoinType);
    }

    @Override
    public double scanSelectivity(Optimizable innerTable) throws StandardException{
        return andNode.getLeftOperand().scanSelectivity(innerTable);
    }

    @Override
    public int getIndexPosition(){
        return indexPosition;
    }


    /* Comparable interface */
    @SuppressFBWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS", justification = "Intentional")
    @Override
    public int compareTo(Predicate otherPred){
		/* Not all operators are "equal". If the predicates are on the
		 * same key column, then a "=" opertor takes precedence over all
		 * other operators.  This ensures that the "=" will be both the start
		 * and stop predicates.  Otherwise, we could end up with it being one
		 * but not the other and get incorrect results.
		 *
		 * Also, we want "<>" to come after all the other operators.
		 * Other parts of the optimizer use the first predicate on an index
		 * column to determine the cost of using the index, so we want the
		 * "<>" to come last because it's not useful for limiting the scan.
		 *
		 * In other words, P1 is before() P2 if:
		 *		o  The P1.indexPosition < P2.indexPosition
		 *	or  o  P1.indexPosition == P2.indexPosition and
		 *		   P1's operator is ("=" or IS NULL) and
		 *		   P2's operator is not ("=" or IS NULL)
		 * or	o  P1.indexPosition == P2.indexPosition and
		 *		   P1's operator is not ("<>" or IS NOT NULL) and
		 *		   P2's operator is ("<>" or IS NOT NULL)
		 *
		 * (We have to impose an arbitrary, but reproducible ordering
		 * on the the "=" predicates on the same column, otherwise an
		 * ASSERTion, that after the predicates are order, Pn+1 is not
		 * before() Pn, will be violated.
		 */

        int otherIndexPosition=otherPred.getIndexPosition();

        if(indexPosition<otherIndexPosition)
            return -1;

        if(indexPosition>otherIndexPosition)
            return 1;

        // initialize these flags as if they are for "in" operator, then
        // change them if they are not
        //
        boolean thisIsEquals=false, otherIsEquals=false;
        boolean thisIsNotEquals=true, otherIsNotEquals=true;
        boolean thisEqualsConstant = false, otherEqualsConstant=false;

		/* The call to "isRelationalOpPredicate()" will return false
		 * for a "probe predicate" because a probe predicate is really
		 * a disguised IN list. But when it comes to sorting, the probe
		 * predicate (which is of the form "<col> = ?") should be treated
		 * as an equality--i.e. it should have precedence over any non-
		 * equals predicate, per the comment at the start of this
		 * method.  So that's what we're checking here.
		 */
        //this is not "in" or this is a probe predicate
        if(this.isRelationalOpPredicate() || this.isInListProbePredicate()){
            int thisOperator=((RelationalOperator)andNode.getLeftOperand()).getOperator();
            thisIsEquals=(thisOperator==RelationalOperator.EQUALS_RELOP ||
                    thisOperator==RelationalOperator.IS_NULL_RELOP);
            thisIsNotEquals=(thisOperator==RelationalOperator.NOT_EQUALS_RELOP ||
                    thisOperator==RelationalOperator.IS_NOT_NULL_RELOP);
            thisEqualsConstant = thisIsEquals &&
                    (this.compareWithKnownConstant(null, true) || this.isInListProbePredicate());
        }
        // other is not "in" or other is a probe predicate
        if(otherPred.isRelationalOpPredicate() || otherPred.isInListProbePredicate()){
            int otherOperator=((RelationalOperator)(otherPred.getAndNode().getLeftOperand())).getOperator();
            otherIsEquals=(otherOperator==RelationalOperator.EQUALS_RELOP ||
                    otherOperator==RelationalOperator.IS_NULL_RELOP);
            otherIsNotEquals=(otherOperator==RelationalOperator.NOT_EQUALS_RELOP ||
                    otherOperator==RelationalOperator.IS_NOT_NULL_RELOP);
            otherEqualsConstant = otherIsEquals &&
                    (otherPred.compareWithKnownConstant(null,true) || otherPred.isInListProbePredicate());
        }

        boolean thisIsBefore=(thisIsEquals && !otherIsEquals) || (!thisIsNotEquals && otherIsNotEquals);
        if(thisIsBefore)
            return -1;

        boolean otherIsBefore=(otherIsEquals && !thisIsEquals) || (!otherIsNotEquals && thisIsNotEquals);
        if(otherIsBefore)
            return 1;

        if (thisEqualsConstant && !otherEqualsConstant)
            return -1;

        if (otherEqualsConstant && !thisEqualsConstant)
            return 1;

        return 0;
    }

    /**
     * Return the andNode.
     *
     * @return AndNode    The andNode.
     */
    public AndNode getAndNode(){
        return andNode;
    }

    /**
     * Set the andNode.
     *
     * @param andNode The new andNode.
     */
    public void setAndNode(AndNode andNode){
        this.andNode=andNode;
    }

    /**
     * Return the pushable.
     *
     * @return boolean    Whether or not the predicate is pushable.
     */
    public boolean getPushable(){
        return pushable;
    }

    /**
     * Set whether or not this predicate is pushable.  This method
     * is intended for use when creating a copy of the predicate, ex
     * for predicate pushdown.  We choose not to add this assignment
     * to copyFields() because the comments for that method say that
     * it should copy all fields _except_ the two specified at init
     * time; "pushable" is one of the two specified at init time.
     *
     * @param pushable Whether or not the predicate is pushable.
     */
    public void setPushable(boolean pushable){
        this.pushable=pushable;
    }

    /**
     * Return the referencedSet.
     *
     * @return JBitSet    The referencedSet.
     */
    public JBitSet getReferencedSet(){
        return referencedSet;
    }

    /**
     * Set the equivalence class, if any, for this predicate.
     *
     * @param equivalenceClass The equivalence class for this predicate.
     */
    void setEquivalenceClass(int equivalenceClass){
        this.equivalenceClass=equivalenceClass;
    }

    /**
     * Get the equivalenceClass for this predicate.
     *
     * @return The equivalenceClass for this predicate.
     */
    int getEquivalenceClass(){
        return equivalenceClass;
    }

    /**
     * Categorize this predicate.  Initially, this means
     * building a bit map of the referenced tables for each predicate.
     *
     * @throws StandardException Thrown on error
     */
    public void categorize() throws StandardException{
        pushable=andNode.categorize(referencedSet,false);
    }

    /**
     * Get the RelationalOperator on the left side of the AND node, if
     * there is one.  If the left side is not a RelationalOperator, return
     * null.
     *
     * @return The RelationalOperator on the left side of the AND node,
     * if any.
     */
    public RelationalOperator getRelop(){
        if(andNode.getLeftOperand() instanceof RelationalOperator){
            return (RelationalOperator)andNode.getLeftOperand();
        }else{
            return null;
        }
    }

    public final boolean isOrList(){
        return (andNode.getLeftOperand() instanceof OrNode) || (andNode.getLeftOperand() instanceof InListOperatorNode);
    }

    /**
     * Is this predicate a possible Qualifier for store?
     * <p/>
     * Current 2 types of predicates can be pushed to store:
     * 1) RelationalOperator -
     * represented with by left operand as instance of RelationalOperator.
     * 2) A single And'd term of a list of OR terms
     * represented by left operand as instance of OrNode.
     * <p/>
     * More checking specific operator's terms to see if they are finally
     * pushable to store.  In the final push at execution each term of the AND
     * or OR must be a Relational operator with a column reference on one side
     * and a constant on the other.
     *
     * @return true if term is wither a AND of a RelationalOperator, or an
     * OR of one or more Relational Operators.
     */
    public final boolean isStoreQualifier(){
        return (andNode.getLeftOperand() instanceof RelationalOperator) ||
                (andNode.getLeftOperand() instanceof OrNode);
    }

    /**
     * Is this predicate an pushable OR list?
     * <p/>
     * Does the predicate represent a AND'd list of OR term's, all of which
     * are pushable.  To be pushable each of OR terms must be a legal
     * qualifier, which is a column reference on one side of a Relational
     * operator and a constant on the other.
     *
     * @return true if the predicate is a pushable set of OR clauses.
     * @throws StandardException Standard exception policy.
     */
    public final boolean isPushableOrClause(Optimizable optTable) throws StandardException{

        if(andNode.getLeftOperand() instanceof OrNode){
            QueryTreeNode node=andNode.getLeftOperand();

            while(node instanceof OrNode){
                OrNode or_node=(OrNode)node;

                if(or_node.getLeftOperand() instanceof RelationalOperator){
                    // if any term of the OR clause is not a qualifier, then
                    // reject the entire OR clause.
                    if(!((RelationalOperator)or_node.getLeftOperand()).isQualifier(optTable,true)){
                        // one of the terms is not a pushable Qualifier.
                        return (false);
                    }

                    node=or_node.getRightOperand();
                }else{
                    // one of the terms is not a RelationalOperator

                    return (false);
                }
            }

            return (true);
        }else{
            // Not an OR list
            return (false);
        }
    }

    /**
     * Return whether or not this predicate has been used
     * to add a new search clause of the specified type via transitive closure.
     * NOTE: This can only be true if this is an equijoin
     * between 2 column references.
     *
     * @param ro The search clause that we are currently considering
     *           as the source for transitive closure
     * @return Whether or not this predicate has been used
     * to add a new search clause of the specified type via transitive
     * closure.
     */
    boolean transitiveSearchClauseAdded(RelationalOperator ro){
        return !(searchClauseHT==null || searchClauseHT.get(ro.getOperator())==null);
    }

    /**
     * Mark this predicate as having been used to add a new predicate
     * of the specified type via transitive closure on search clauses.
     *
     * @param ro The search clause that we are currently considering
     *           as the source for transitive closure
     */
    void setTransitiveSearchClauseAdded(RelationalOperator ro){
        if(searchClauseHT==null){
            searchClauseHT=new Hashtable();
        }
		/* I have to remember that this ro has been added to this predicate as a
		 * transitive search clause.
		 */
        //noinspection UnnecessaryBoxing
        Integer i=Integer.valueOf(ro.getOperator());
        //noinspection unchecked
        searchClauseHT.put(i,i);
    }

    /**
     * Get the start operator for this predicate for a scan.
     *
     * @param optTable The optimizable table, so we can tell which side of
     *                 the operator the search column is on.
     * @return The start operator for a start key on this column.
     */
    int getStartOperator(Optimizable optTable){
        assert startKey: "Getting a start operator from a Predicate that's not a start key.";

		/* if it's for "in" operator's dynamic start key, operator is GE,
		 * beetle 3858
		 */
        if(andNode.getLeftOperand() instanceof InListOperatorNode)
            return ScanController.GE;

        RelationalOperator relop=getRelop();
        assert relop!=null;

        return relop.getStartOperator(optTable);
    }

    int getStopOperator(Optimizable optTable){
        assert stopKey: "Getting a stop operator from a Predicate that's not a stop key.";

		 /* if it's for "in" operator's dynamic stop key, operator is GT,
		  * beetle 3858
		  */
        if(andNode.getLeftOperand() instanceof InListOperatorNode)
            return ScanController.GT;

        RelationalOperator relop=getRelop();
        assert relop!=null;
        return relop.getStopOperator(optTable);
    }

    /**
     * Set the position of the index column that this predicate restricts
     *
     * @param indexPosition The position of the index column that this
     *                      predicate restricts.
     */
    void setIndexPosition(int indexPosition){
        this.indexPosition=indexPosition;
    }

    /**
     * Clear the start/stop position and qualifier flags
     */
    public void clearScanFlags(){
        startKey=false;
        stopKey=false;
        isQualifier=false;
        // indexPosition of -1 is used by rowId, so use -2 to indicate that it has been set
        indexPosition = -2;
    }

    void generateExpressionOperand(Optimizable optTable,
                                   int columnPosition,
                                   ExpressionClassBuilder acb,
                                   MethodBuilder mb) throws StandardException{
        RelationalOperator relop=getRelop();
        assert relop!=null;
        relop.generateExpressionOperand(optTable,columnPosition,acb,mb);
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
            return binaryRelOpColRefsToString()+"\nreferencedSet: "+
                    referencedSet+"\n"+"pushable: "+pushable+"\n"+
                    "isQualifier: "+isQualifier+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }

    /**
     * Get a string version of the column references for this predicate
     * IF it's a binary relational operator.  We only print out the
     * names of the operands if they are column references; otherwise
     * we just print a dummy value.  This is for debugging purposes
     * only--it's a convenient way to see what columns the predicate
     * is referencing, especially when tracing through code and printing
     * assert failure.
     */
    public String binaryRelOpColRefsToString(){
        // We only consider binary relational operators here.
        if(!(getAndNode().getLeftOperand()
                instanceof BinaryRelationalOperatorNode)){
            return "";
        }

        final String DUMMY_VAL="<expr>";
        StringBuilder sBuf=new StringBuilder();
        BinaryRelationalOperatorNode opNode=(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

        // Get left operand's name.
        if(opNode.getLeftOperand() instanceof ColumnReference){
            sBuf.append(opNode.getLeftOperand().getTableName())
                    .append(".")
                    .append(opNode.getLeftOperand().getColumnName());
        }else
            sBuf.append(DUMMY_VAL);

        // Get the operator type.
        sBuf.append(" ").append(opNode.operator).append(" ");

        // Get right operand's name.
        if(opNode.getRightOperand() instanceof ColumnReference){
            sBuf.append(opNode.getRightOperand().getTableName())
                    .append(".")
                    .append(opNode.getRightOperand().getColumnName());
        }else
            sBuf.append(DUMMY_VAL);

        return sBuf.toString();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    @Override
    public void printSubNodes(int depth){
        if(SanityManager.DEBUG){
            printLabel(depth,"andNode: ");
            andNode.treePrint(depth+1);
            super.printSubNodes(depth);
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        // Don't replace the top-level AND.
        if(andNode!=null){
            if (v instanceof PredicateSimplificationVisitor) {
                andNode.accept(v, this);
            }
            else
                andNode=(AndNode)andNode.accept(v, this);
        }
    }

    /**
     * Copy all fields of this Predicate (except the two that
     * are set from 'init').
     */

    public void copyFields(Predicate otherPred){

        this.equivalenceClass=otherPred.getEquivalenceClass();
        this.indexPosition=otherPred.getIndexPosition();
        this.startKey=otherPred.isStartKey();
        this.stopKey=otherPred.isStopKey();
        this.isQualifier=otherPred.isQualifier();
        this.searchClauseHT=otherPred.getSearchClauseHT();

    }

    /**
     * Get the search clause Hash Table.
     */

    public Hashtable getSearchClauseHT(){
        return searchClauseHT;
    }

    /**
     * Determine whether or not this predicate is eligible for
     * push-down into subqueries.  Right now the only predicates
     * we consider to be eligible are those which 1) are Binary
     * Relational operator nodes and 2) have a column reference
     * on BOTH sides, each of which has a reference to a base
     * table somewhere beneath it.
     *
     * @return Whether or not this predicate is eligible to be
     * pushed into subqueries.
     */
    protected boolean pushableToSubqueries() throws StandardException{
        if(!isJoinPredicate(false))
            return false;

        // isJoinPredicate() only treats BinaryRelationalOperatorNode as JoinPredicate, so when we get it
        // it has to be of this type
        BinaryRelationalOperatorNode opNode=(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

        /* getScopedOperand() currently only expecting ColumnReferences, so return false if they are not */
        if (!(opNode.getLeftOperand() instanceof ColumnReference) ||
                !(opNode.getRightOperand() instanceof ColumnReference)) {
            return false;
        }
        if (isFullJoinPredicate())
            return false;

        // Make sure both column references ultimately point to base
        // tables.  If, for example, either column reference points to a
        // a literal or an aggregate, then we do not push the predicate.
        // This is because pushing involves remapping the references--
        // but if the reference doesn't have a base table beneath it,
        // the notion of "remapping" it doesn't (seem to) apply.  RESOLVE:
        // it might be okay to make the "remap" operation a no-op for
        // such column references, but it's not clear whether that's
        // always a safe option; further investigation required.

        JBitSet tNums=new JBitSet(getReferencedSet().size());
        BaseTableNumbersVisitor btnVis=new BaseTableNumbersVisitor(tNums);
        opNode.getLeftOperand().accept(btnVis);
        if(tNums.getFirstSetBit()==-1)
            return false;

        tNums.clearAll();
        opNode.getRightOperand().accept(btnVis);
        return tNums.getFirstSetBit()!=-1;

    }

    public boolean isJoinPredicate(){
        return isJoinPredicate(true);
    }

    /**
     * Is this predicate a join predicate?  In order to be so,
     * it must be a binary relational operator node that has
     * a column reference on both sides.
     *
     * @return Whether or not this is a join predicate.
     */
    public boolean isJoinPredicate(boolean checkScope){
        // If the predicate isn't a binary relational operator,
        // then it's not a join predicate.
        if(!(getAndNode().getLeftOperand() instanceof BinaryRelationalOperatorNode)){
            return false;
        }

        BinaryRelationalOperatorNode opNode=(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

        ValueNode leftOperand=opNode.getLeftOperand();
        ValueNode rightOperand=opNode.getRightOperand();
        boolean isColumnReferenceOnLeft=false;
        boolean isColumnReferenceOnRight=false;
        int leftTableNumber=-1;
        int rightTableNumber=-1;

        List<ColumnReference> columnReferences = leftOperand.getHashableJoinColumnReference();
        if (columnReferences != null && columnReferences.size() == 1) {
            isColumnReferenceOnLeft = true;
            leftTableNumber = columnReferences.get(0).getTableNumber();
        }

        columnReferences = rightOperand.getHashableJoinColumnReference();
        if (columnReferences != null && columnReferences.size() == 1) {
            isColumnReferenceOnRight = true;
            rightTableNumber = columnReferences.get(0).getTableNumber();
        }

        // If both sides are column references AND they point to different
        // tables, then this is a join pred.
        // Scoped predicates are nestedloop join predicate that are pushed down, if checkScope is true,
        // we should no longer treat them as join condition, but single table condition instead
        return (isColumnReferenceOnLeft &&
                isColumnReferenceOnRight &&
                leftTableNumber!=rightTableNumber) &&
                (!checkScope  || !isScopedForPush());
    }

    public void markFullJoinPredicate(boolean isForFullJoin) {
        forFullJoin = isForFullJoin;
    }

    /* For full join, full join predicate is a superset of isJoinPredicate(), as a single table condition
       can also appear on the ON clause of a full outer join. For example:
       select 1 from t1 full join t2 on a1=a2 and a2=1;
       a2=1 cannot be applied as a single table condition but a join condition.
     */
    public boolean isFullJoinPredicate() {
        return forFullJoin;
    }

    /**
     * If this predicate's operator is a BinaryRelationalOperatorNode,
     * then look at the operands and return a new, equivalent predicate
     * that is "scoped" to the received ResultSetNode.  By "scoped" we
     * mean that the operands, which shold be column references, have been
     * mapped to the appropriate result columns in the received RSN.
     * This is useful for pushing predicates from outer queries down
     * into inner queries, in which case the column references need
     * to be remapped.
     * <p/>
     * For example, let V1 represent
     * <p/>
     * select i,j from t1 UNION select i,j from t2
     * <p/>
     * and V2 represent
     * <p/>
     * select a,b from t3 UNION select a,b from t4
     * <p/>
     * Then assume we have the following query:
     * <p/>
     * select * from V1, V2 where V1.j = V2.b
     * <p/>
     * Let's further assume that this Predicate object represents the
     * "V1.j = V2.b" operator and that the childRSN we received
     * as a parameter represents one of the subqueries to which we
     * want to push the predicate; let's say it's:
     * <p/>
     * select i,j from t1
     * <p/>
     * Then this method will return a new predicate whose binary
     * operator represents the expression "T1.j = V2.b" (that is, V1.j
     * will be mapped to the corresponding column in T1).  For more on
     * how that mapping is made, see the "getScopedOperand()" method
     * in BinaryRelationalOperatorNode.java.
     * <p/>
     * ASSUMPTION: We should only get to this method if we know that
     * at least one operand in this predicate can and should be mapped
     * to the received childRSN.  For an example of where that check is
     * made, see the pushOptPredicate() method in SetOperatorNode.java.
     *
     * @param parentRSNsTables Set of all table numbers referenced by
     *                         the ResultSetNode that is _parent_ to the received childRSN.
     *                         We need this to make sure we don't scope the operands to a
     *                         ResultSetNode to which they don't apply.
     * @param childRSN         The result set node for which we want to create
     *                         a scoped predicate.
     * @param whichRC          If not -1 then this tells us which ResultColumn
     *                         in the received childRSN we need to use for the scoped predicate;
     *                         if -1 then the column position of the scoped column reference
     *                         will be stored in this array and passed back to the caller.
     * @return A new predicate whose operands have been scoped to the
     * received childRSN.
     */
    protected Predicate getPredScopedForResultSet(JBitSet parentRSNsTables,
                                                  ResultSetNode childRSN,
                                                  int[] whichRC) throws StandardException{
        // We only deal with binary relational operators here.
        if(!(getAndNode().getLeftOperand() instanceof BinaryRelationalOperatorNode)){
            return this;
        }

        BinaryRelationalOperatorNode opNode=(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

        // Create a new op node with left and right operands that point
        // to the received result set's columns as appropriate.
        ValueNode newLeftOpNode = opNode.getScopedOperand(
                BinaryRelationalOperatorNode.LEFT,
                parentRSNsTables,
                childRSN,
                whichRC);
        ValueNode newRightOpNode = opNode.getScopedOperand(
                BinaryRelationalOperatorNode.RIGHT,
                parentRSNsTables,
                childRSN,
                whichRC);

        // for a scoped predicate, one side of the operand needs to be scoped,
        // see function Predicate.isScopedToSourceResultSet(). Currently we cannot scope
        // the operand if its souce is either a constant or expression, aggregation, we
        // may end up with no scoped operand for either side. So return here without doing
        // useless work further
        if ((!(newLeftOpNode instanceof ColumnReference) ||
                !((ColumnReference)newLeftOpNode).isScoped()) &&
             (!(newRightOpNode instanceof ColumnReference) ||
                     !((ColumnReference)newRightOpNode).isScoped()))
            return this;

        BinaryRelationalOperatorNode newOpNode=(BinaryRelationalOperatorNode)getNodeFactory().getNode(
                opNode.getNodeType(),
                newLeftOpNode,
                newRightOpNode,
                getContextManager());

        // Bind the new op node.
        newOpNode.bindComparisonOperator();

        // The predicate must have an AndNode in CNF, so we
        // need to create an AndNode representing:
        //    <scoped_bin_rel_op> AND TRUE
        // First create the boolean constant for TRUE.
        ValueNode trueNode=(ValueNode)getNodeFactory().getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                Boolean.TRUE,
                getContextManager());

        // Create and bind a new AND node in CNF form,
        // i.e. "<newOpNode> AND TRUE".
        AndNode newAnd=(AndNode)getNodeFactory().getNode(
                C_NodeTypes.AND_NODE,
                newOpNode,
                trueNode,
                getContextManager());
        newAnd.postBindFixup();

        // Categorize the new AND node; among other things, this
        // call sets up the new operators's referenced table map,
        // which is important for correct pushing of the new
        // predicate.
        JBitSet tableMap=new JBitSet(childRSN.getReferencedTableMap().size());
        newAnd.categorize(tableMap,false);

        // Now put the pieces together to get a new predicate.
        Predicate newPred=(Predicate)getNodeFactory().getNode(C_NodeTypes.PREDICATE,
                newAnd,
                tableMap,
                getContextManager());

        // Copy all of this predicates other fields into the new predicate.
        newPred.clearScanFlags();
        newPred.copyFields(this);
        newPred.setPushable(getPushable());

        // Take note of the fact that the new predicate is scoped for
        // the sake of pushing; we need this information during optimization
        // to figure out what we should and should not "pull" back up.
        newPred.markAsScopedForPush();
        return newPred;
    }

    /**
     * Indicate that this predicate is a scoped copy of some other
     * predicate (i.e. it was created as the result of a call to
     * getPredScopedForResultSet() on some other predicate).
     */
    protected void markAsScopedForPush(){
        this.scoped=true;
    }

    /**
     * Return whether or not this predicate is a scoped copy of
     * another predicate.
     */
    public boolean isScopedForPush(){
        return scoped;
    }

    /**
     * When remapping a "normal" (i.e. non-scoped) predicate both
     * of the predicate's operands are remapped and that's it.
     * But when remapping a scoped predicate, things are slightly
     * different.  This method handles remapping of scoped predicates.
     * <p/>
     * We know that, for a scoped predicate, exactly one operand has
     * been scoped for a specific target result set; the other operand
     * is pointing to some other instance of FromTable with which the
     * target result set is to be joined (see getScopedOperand() in
     * BinaryRelationalOperatorNode.java).  For every level of the
     * query through which the scoped predicate is pushed, we have
     * to perform a remap operation of the scoped operand.  We do
     * *not*, however, remap the non-scoped operand.  The reason
     * is that the non-scoped operand is already pointing to the
     * result set against which it must be evaluated.  As the scoped
     * predicate is pushed down the query tree, the non-scoped
     * operand should not change where it's pointing and thus should
     * not be remapped.  For example, assume we have a query whose
     * tree has the following form:
     * <p/>
     * SELECT[0]
     * /     \
     * PRN      PRN
     * |        |
     * SELECT[4]   UNION
     * |           /   \
     * PRN     SELECT[1]  SELECT[2]
     * |         |          |
     * <FBT:T1>     PRN        PRN
     * |          |
     * SELECT[3]  <FromBaseTable:T2>
     * |
     * PRN
     * |
     * <FromBaseTable:T3>
     * <p/>
     * Assume also that we have some predicate "SELECT[4].i = <UNION>.j".
     * If the optimizer decides to push the predicate to the UNION
     * node, it (the predicate) will be scoped to the UNION's children,
     * yielding something like "SELECT[4].i = SELECT[1].j" for the
     * left child and "SELECT[4].i = SELECT[2].j" for the right child.
     * These scoped predicates will then be pushed to the PRNs above
     * SELECT[3] and T2, respectively.  As part of that pushing
     * process a call to PRN.pushOptPredicate() will occur, which
     * brings us to this method.  So let's assume we're here for
     * the scoped predicate "SELECT[4].i = SELECT[1].j".  Then we want
     * to remap the scoped operand, "SELECT[1].j", so that it will
     * point to the correct column in "SELECT[3]".  We do NOT, however,
     * want to remap the non-scoped operand "SELECT[4].i" because that
     * operand is already pointing to the correct result set--namely,
     * to a column in SELECT[4].  That non-scoped operand should not
     * change regardless of how far down the UNION subtree the scoped
     * predicate is pushed.
     * <p/>
     * If we did try to remap the non-scoped operand, it would end up
     * pointing to result sets too low in the tree, which could lead to
     * execution-time errors.  So when we remap a scoped predicate, we
     * have to make sure we only remap the scoped operand.  That's what
     * this method does.
     *
     * @return True if this predicate is a scoped predicate, in which
     * case we performed a one-sided remap.  False if the predicate is
     * not scoped; the caller can then make the calls to perform a
     * "normal" remap on this predicate.
     */
    protected boolean remapScopedPred(){
        if(!scoped)
            return false;

		/* Note: right now the only predicates we scope are those
		 * which are join predicates and all scoped predicates will
		 * have the same relational operator as the predicates from
		 * which they were scoped.  Thus if we get here, we know
		 * that andNode's leftOperand must be an instance of
		 * BinaryRelationalOperatorNode (and therefore the following
		 * cast is safe).
		 */
        BinaryRelationalOperatorNode binRelOp=(BinaryRelationalOperatorNode)andNode.getLeftOperand();

        ValueNode operand;

        if(SanityManager.DEBUG){
			/* If this predicate is scoped then one (and only one) of
			 * its operands should be scoped.  Note that it's possible
			 * for an operand to be scoped to a non-ColumnReference
			 * value; if either operand is not a ColumnReference, then
			 * that operand must be the scoped operand.
			 */
            operand=binRelOp.getLeftOperand();
            boolean leftIsScoped=!(operand instanceof ColumnReference) || ((ColumnReference)operand).isScoped();

            operand=binRelOp.getRightOperand();
            boolean rightIsScoped=!(operand instanceof ColumnReference) || ((ColumnReference)operand).isScoped();

            SanityManager.ASSERT(leftIsScoped^rightIsScoped,
                    "All scoped predicates should have exactly one scoped "+
                            "operand, but '"+binaryRelOpColRefsToString()+
                            "' has "+(leftIsScoped?"TWO":"NONE")+".");
        }

        // Find the scoped operand and remap it.
        operand=binRelOp.getLeftOperand();
        if((operand instanceof ColumnReference) && ((ColumnReference)operand).isScoped()){
            // Left operand is the scoped operand.
            ((ColumnReference)operand).remapColumnReferences();
        }else{
            operand=binRelOp.getRightOperand();
            if((operand instanceof ColumnReference) && ((ColumnReference)operand).isScoped()){
                // Right operand is the scoped operand.
                ((ColumnReference)operand).remapColumnReferences();
            }

            // Else scoped operand is not a ColumnReference, which
            // means it can't (and doesn't need to) be remapped. So
            // just fall through and return.
        }

        return true;
    }

    /**
     * Return true if this predicate is scoped AND the scoped
     * operand is a ColumnReference that points to a source result
     * set.  If the scoped operand is not a ColumnReference that
     * points to a source result set then it must be pointing to
     * some kind of expression, such as a literal (ex. 'strlit'),
     * an aggregate value (ex. "count(*)"), or the result of a
     * function (ex. "sin(i)") or operator (ex. "i+1").
     * <p/>
     * This method is used when pushing predicates to determine how
     * far down the query tree a scoped predicate needs to be pushed
     * to allow for successful evaluation of the scoped operand.  If
     * the scoped operand is not pointing to a source result set
     * then it should not be pushed any further down tree.  The reason
     * is that evaluation of the expression to which the operand is
     * pointing may depend on other values from the current level
     * in the tree (ex. "sin(i)" depends on the value of "i", which
     * could be a column at the predicate's current level).  If we
     * pushed the predicate further down, those values could become
     * inaccessible, leading to execution-time errors.
     * <p/>
     * If, on the other hand, the scoped operand *is* pointing to
     * a source result set, then we want to push it further down
     * the tree until it reaches that result set, which allows
     * evaluation of this predicate to occur as close to store as
     * possible.  This method doesn't actually do the push, it just
     * returns "true" and then the caller can push as appropriate.
     */
    public boolean isScopedToSourceResultSet() throws StandardException{
        if(!scoped)
            return false;

		/* Note: right now the only predicates we scope are those
		 * which are join predicates and all scoped predicates will
		 * have the same relational operator as the predicates from
		 * which they were scoped.  Thus if we get here, we know
		 * that andNode's leftOperand must be an instance of
		 * BinaryRelationalOperatorNode (and therefore the following
		 * cast is safe).
		 */
        BinaryRelationalOperatorNode binRelOp=(BinaryRelationalOperatorNode)andNode.getLeftOperand();

        ValueNode operand=binRelOp.getLeftOperand();

		/* If operand isn't a ColumnReference then is must be the
		 * scoped operand.  This is because both operands have to
		 * be column references in order for scoping to occur (as
		 * per pushableToSubqueries()) and only the scoped operand
		 * can change (esp. can become a non-ColumnReference) as
		 * part of the scoping process.  And since it's not a
		 * ColumnReference it can't be "a ColumnReference that
		 * points to a source result set", so return false.
		 */
        if(!(operand instanceof ColumnReference))
            return false;

		/* If the operand is a ColumnReference and is scoped,
		 * then see if it is pointing to a ResultColumn whose
		 * expression is either another a CR or a Virtual
		 * ColumnNode.  If it is then that operand applies
		 * to a source result set further down the tree and
		 * thus we return true.
		 */
        ValueNode exp;
        ColumnReference cRef=(ColumnReference)operand;
        if(cRef.isScoped()){
            exp=cRef.getSource().getExpression();
            return ((exp instanceof VirtualColumnNode) || (exp instanceof ColumnReference));
        }

        operand=binRelOp.getRightOperand();
        if(!(operand instanceof ColumnReference))
            return false;

        cRef=(ColumnReference)operand;
        if(SanityManager.DEBUG){
            // If we got here then the left operand was NOT the scoped
            // operand; make sure the right one is scoped, then.
            SanityManager.ASSERT(cRef.isScoped(),
                    "All scoped predicates should have exactly one scoped "+
                            "operand, but '"+binaryRelOpColRefsToString()+
                            "has NONE.");
        }

        exp=cRef.getSource().getExpression();
        return ((exp instanceof VirtualColumnNode) || (exp instanceof ColumnReference));
    }

    /**
     * Return whether or not this predicate corresponds to a legitimate
     * relational operator.
     *
     * @return False if there is no relational operator for this predicate
     * OR if this predicate is an internal "probe predicate" (in which
     * case it "looks" like we have a relational operator but in truth
     * it's a disguised IN-list operator). True otherwise.
     */
    protected boolean isRelationalOpPredicate(){
		/* The isRelationalOperator() method on the ValueNode
		 * interface tells us what we need to know, so all we have
		 * to do is call that method on the left child of our AND node.
		 * Note that BinaryRelationalOperatorNode.isRelationalOperator()
		 * includes logic to determine whether or not it (the BRON) is
		 * really a disguised IN-list operator--and if so, it will
		 * return false (which is what we want).
		 */
        return andNode.getLeftOperand().isRelationalOperator();
    }

    /**
     * Return whether or not this predicate is an IN-list probe
     * predicate.
     */
    public boolean isInListProbePredicate(){
		/* The isInListProbeNode() method on the ValueNode interface
		 * tells us what we need to know, so all we have to do is call
		 * that method on the left child of our AND node.
		 */
        return andNode.getLeftOperand().isInListProbeNode();
    }

    /**
     * If this predicate corresponds to an IN-list, return the underlying
     * InListOperatorNode from which it was built.  There are two forms
     * to check for:
     * <p/>
     * 1. This predicate is an IN-list "probe predicate", in which case
     * the underlying InListOpNode is stored within the binary relational
     * operator that is the left operand of this predicate's AND node.
     * <p/>
     * 2. This predicate corresponds to an IN-list that could _not_ be
     * transformed into a "probe predicate" (i.e. the IN-list contains
     * one or more non-parameter, non-constant values). In that case
     * the underlying InListOpNode is simply the left operand of
     * this predicate's AND node.
     * <p/>
     * If this predicate does not correspond to an IN-list in any way,
     * this method will return null.
     */
    public InListOperatorNode getSourceInList() throws StandardException{
        return getSourceInList(false);
    }

    /**
     * Does the work of getSourceInList() above, but can also be called
     * directly with an argument to indicate whether or not we should
     * limit ourselves to probe predicates.
     *
     * @param probePredOnly If true, only get the source IN list for this
     *                      predicate *if* it is an IN-list probe predicate.  If false,
     *                      return the underlying InListOperatorNode (if it exists) regardless
     *                      of whether this is a probe predicate or an un-transformed IN-list
     *                      pred.
     * @return Underlying InListOp for this predicate (depending on
     * the value of probePredOnly), or null if this predicate does
     * not correspond to an IN-list in any way.
     */
    protected InListOperatorNode getSourceInList(boolean probePredOnly) throws StandardException{
        ValueNode vn=andNode.getLeftOperand();
        if(isInListProbePredicate())
            return ((BinaryRelationalOperatorNode)vn).getInListOp();

        if(probePredOnly)
            return null;

        if(vn instanceof InListOperatorNode)
            return (InListOperatorNode)vn;

        return null;
    }
    
    /**
     * Returns the number of columns represented in a qualifier predicate if the
     * predicate is a probe predicate, otherwise returns 1.
     */
    protected int numColumnsInQualifier() throws StandardException {
        InListOperatorNode ilon = getSourceInList();
        return (ilon == null) ? 1 : ilon.leftOperandList.size();
    }

    /**
     * Returns true if the predicate is a multi-probe qualifier.
     *
     * A Multi-probe qualifier satisfies 2 criteria:
     * 1. It has an IN clause
     * 2. There are only constant nodes in the IN list
     *
     * With DB-6231's enhancement, inlist does not need to be on the first index column anymore
     */
    public boolean isMultiProbeQualifier(int[] keyColumns) throws StandardException{
        if(keyColumns==null)
            return false; //can't be a MPQ if there are no keyed columns
        InListOperatorNode sourceInList=getSourceInList();
        if(sourceInList==null)
            return false; //not a multi-probe predicate
        for (int i = 0; i < sourceInList.leftOperandList.size(); i++) {
            ValueNode lo = (ValueNode) sourceInList.leftOperandList.elementAt(i);
            //if it doesn't refer to a column, then it can't be a qualifier
            if (!(lo instanceof ColumnReference))
                return false;
        }

        //if inlist is eligible for multiprobe index scan, its indexPosition will be set(>=0) and
        //this predicate can be pushed down to base table
        if(getIndexPosition() < 0)
            return false;

        ValueNodeList rightOperandList=sourceInList.getRightOperandList();
        for(Object o:rightOperandList){
            if(!(o instanceof ConstantNode))
                return false; //not all constants in the IN list
        }
        return true;
    }

    /**
     * Returns true if the predicate is a multi-probe qualifier.
     *
     * A Multi-probe qualifier satisfies 3 criteria:
     * 1. It has an IN clause
     * 2. It is applied against the first keyed column
     * 3. There are only constant nodes in the IN list
     */
    public boolean isInQualifier(BitSet bitSet) throws StandardException {
        if (bitSet == null)
            return false;
        InListOperatorNode sourceInList=getSourceInList();
        if(sourceInList==null)
            return false; //not a multi-probe predicate
        for (Object o : sourceInList.getLeftOperandList()) {
            ValueNode lo = (ValueNode) o;
            //if it doesn't refer to a column, then it can't be a qualifier
            if (!(lo instanceof ColumnReference))
                return false;
            ColumnReference colRef = (ColumnReference) lo;
            int colNum = colRef.getColumnNumber();
            if (!bitSet.get(colNum)) // Miss
                return false;
        }
        ValueNodeList rightOperandList=sourceInList.getRightOperandList();
        for(Object o:rightOperandList){
            if(!(o instanceof ConstantNode)) {
                if (o instanceof ListValueNode) {
                    if (!((ListValueNode)o).allConstantsNodesInList())
                        return false;
                }
                else
                    return false; //not all constants in the IN list
            }
        }
        return true;
    }


    public boolean isNullPredicate() throws StandardException {
        return this.getAndNode() !=null && this.getAndNode().getLeftOperand()!= null && (this.getAndNode().getLeftOperand() instanceof IsNullNode);
    }

    public boolean isUnsatisfiable() {
        if (andNode.getLeftOperand() instanceof BooleanConstantNode) {
            if (andNode.getLeftOperand().isBooleanFalse())
                return true;
        }

        if (isOrList()) {
            //TODO check if every branch is unsatisfiable
            return false;
        }
        return false;
    }

    public static Predicate generateUnsatPredicate(int numTables, NodeFactory nf, ContextManager cm) throws StandardException{
        BooleanConstantNode trueNode=(BooleanConstantNode)nf.
                getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                        Boolean.TRUE,
                        cm);
        BooleanConstantNode falseNode=(BooleanConstantNode)nf.
                getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                        Boolean.FALSE,
                        cm);
        AndNode andNode = (AndNode)nf.
                getNode(C_NodeTypes.AND_NODE,
                        falseNode,
                        trueNode,
                        cm);
        JBitSet newJBitSet=new JBitSet(numTables);

        return (Predicate)nf.
                getNode(C_NodeTypes.PREDICATE,
                        andNode,
                        newJBitSet,
                        cm);
    }
    

    public boolean matchesSingleValue() throws StandardException {
        RelationalOperator relop = getRelop();
        if (relop == null)
            return false;
    
        if (!isRelationalOpPredicate())
            return false;
    
        if (!(relop.getOperator() == RelationalOperator.EQUALS_RELOP ||
              relop.getOperator() == RelationalOperator.IS_NULL_RELOP))
            return false;
        
        if (getSourceInList() != null) {
            if (isInListProbePredicate())
                return true;
            else
                return false;
        }
        
        return true;
    }

    public int getOuterJoinLevel() {
        return outerJoinLevel;
    }

    public void setOuterJoinLevel(int level) {
        outerJoinLevel = level;
    }
}
