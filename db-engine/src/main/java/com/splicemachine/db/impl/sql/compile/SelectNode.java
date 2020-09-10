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
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.CollectingVisitor;
import com.splicemachine.db.impl.ast.ColumnCollectingVisitor;
import com.splicemachine.db.impl.ast.LimitOffsetVisitor;
import splice.com.google.common.base.Predicates;

import java.sql.Types;
import java.util.*;

/**
 * A SelectNode represents the result set for any of the basic DML
 * operations: SELECT, INSERT, UPDATE, and DELETE.  (A RowResultSetNode
 * will be used for an INSERT with a VALUES clause.)  For INSERT - SELECT,
 * any of the fields in a SelectNode can be used (the SelectNode represents
 * the SELECT statement in the INSERT - SELECT).  For UPDATE and
 * DELETE, there will be one table in the fromList, and the groupByList
 * fields will be null. For both INSERT and UPDATE,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 */

public class SelectNode extends ResultSetNode{
    /**
     * List of tables in the FROM clause of this SELECT
     */
    FromList fromList;
    FromTable targetTable;

    /* Aggregate Vectors for select and where clauses */
    List<AggregateNode> selectAggregates;
    List<AggregateNode> whereAggregates;
    List<AggregateNode> havingAggregates;
    /**
     * The ValueNode for the WHERE clause must represent a boolean
     * expression.  The binding phase will enforce this - the parser
     * does not have enough information to enforce it in all cases
     * (for example, user methods that return boolean).
     */
    ValueNode whereClause;
    ValueNode originalWhereClause;

    /**
     * List of result columns in GROUP BY clause
     */
    GroupByList groupByList;
    /* List of columns in ORDER BY list */
    OrderByList orderByList;
    boolean orderByQuery;
    ValueNode offset;  // OFFSET n ROWS, if given
    ValueNode fetchFirst; // FETCH FIRST n ROWS ONLY, if given
    boolean hasJDBClimitClause; //  were OFFSET/FETCH FIRST specified by a JDBC LIMIT clause?
    /* PredicateLists for where clause */
    PredicateList wherePredicates;
    /* SubqueryLists for select where and having clauses */
    SubqueryList selectSubquerys;
    SubqueryList whereSubquerys;
    SubqueryList havingSubquerys;
    boolean originalWhereClauseHadSubqueries;
    ValueNode havingClause;
    /**
     * List of windowDefinitionList.
     */
    private WindowList windowDefinitionList;
    /**
     * List of window function calls (e.g. ROW_NUMBER, AVG(i), DENSE_RANK).
     */
    private List<WindowFunctionNode> windowFuncCalls;
    /**
     * User specified a group by without aggregates and we turned
     * it into a select distinct
     */
    private boolean wasGroupBy;
    /* Whether or not we are only binding the target list */
    private boolean bindTargetListOnly;
    private boolean isDistinct;
    private boolean orderByAndDistinctMerged;
    /* Copy of fromList prior to generating join tree */
    private FromList preJoinFL;
    private int nestingLevel;
    private Satisfiability nonAggregatePartSat = Satisfiability.UNKNOWN;

    public static void checkNoWindowFunctions(QueryTreeNode clause, String clauseName) throws StandardException{
        // Clause cannot contain window functions except inside subqueries
        HasNodeVisitor visitor=new HasNodeVisitor(WindowFunctionNode.class, SubqueryNode.class);
        clause.accept(visitor);

        if(visitor.hasNode()){
            throw StandardException.newException( SQLState.LANG_WINDOW_FUNCTION_CONTEXT_ERROR, clauseName);
        }
    }

    public static void checkNoGroupingFunctions(QueryTreeNode clause, String clauseName) throws StandardException{
        // Clause cannot contain grouping function except inside subqueries
        HasNodeVisitor visitor=new HasNodeVisitor(GroupingFunctionNode.class, SubqueryNode.class);
        clause.accept(visitor);

        if(visitor.hasNode()){
            throw StandardException.newException( SQLState.LANG_GROUPING_FUNCTION_CONTEXT_ERROR, clauseName);
        }
    }

    @Override
    public boolean isParallelizable(){
        return isDistinct
                || (selectAggregates!=null && !selectAggregates.isEmpty())
                || targetTable.isParallelizable();
    }

    @Override
    public CostEstimate getCostEstimate(){
        if(costEstimate==null)
            costEstimate=optimizer.newCostEstimate();
        return costEstimate;
    }

    public void init(Object selectList,
                     Object aggregateVector,
                     Object fromList,
                     Object whereClause,
                     Object groupByList,
                     Object havingClause,
                     Object windowDefinitionList)
            throws StandardException{
        /* RESOLVE - remove aggregateList from constructor.
         * Consider adding selectAggregates and whereAggregates
         */
        resultColumns=(ResultColumnList)selectList;
        if(resultColumns!=null)
            resultColumns.markInitialSize();
        this.fromList=(FromList)fromList;
        this.whereClause=(ValueNode)whereClause;
        this.originalWhereClause=(ValueNode)whereClause;
        this.groupByList=(GroupByList)groupByList;
        this.havingClause=(ValueNode)havingClause;

        // This initially represents an explicit <window definition list>, as
        // opposed to <in-line window specifications>, see 2003, 6.10 and 6.11.
        // <in-line window specifications> are added later, see right below for
        // in-line window specifications used in window functions in the SELECT
        // column list and in genProjectRestrict for such window specifications
        // used in window functions in ORDER BY.
        this.windowDefinitionList = (WindowList)windowDefinitionList;

        bindTargetListOnly=false;

        this.originalWhereClauseHadSubqueries=false;
        if(this.whereClause!=null){
            CollectNodesVisitor cnv= new CollectNodesVisitor(SubqueryNode.class,SubqueryNode.class);
            this.whereClause.accept(cnv);
            if(!cnv.getList().isEmpty()){
                this.originalWhereClauseHadSubqueries=true;
            }
        }

        if(resultColumns!=null){

            // Collect simply contained window functions (note: *not*
            // any inside nested SELECTs) used in result columns, and
            // check them for any <in-line window specification>s.

            CollectNodesVisitor cnvw= new CollectNodesVisitor(WindowFunctionNode.class, SelectNode.class);
            resultColumns.accept(cnvw);
            //-sf- this is safe because we know that only WindowFunctionNodes are in the returned list
            //noinspection unchecked
            windowFuncCalls=cnvw.getList();

            for(WindowFunctionNode wfn : windowFuncCalls){
                // Some window function, e.g. ROW_NUMBER() contains an inline
                // window specification, so we add it to our list of window
                // definitions.

                if(wfn.getWindow() instanceof WindowDefinitionNode){
                    // Window function call contains an inline definition, add
                    // it to our list of windowDefinitionList.
                    this.windowDefinitionList = addInlinedWindowDefinition(this.windowDefinitionList, wfn);
                }else{
                    // a window reference, bind it later.
                    assert wfn.getWindow() instanceof WindowReferenceNode;
                }
            }
        }
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
            return "isDistinct: "+isDistinct+"\n"+ super.toString();
        }else{
            return "";
        }
    }

    public String statementToString(){ return "SELECT"; }

    public void makeDistinct(){ isDistinct=true; }

    public void clearDistinct(){ isDistinct=false; }

    public int getNestingLevel(){
        return nestingLevel;
    }
    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth){
        if(SanityManager.DEBUG){
            super.printSubNodes(depth);

            if(selectSubquerys!=null){
                printLabel(depth,"selectSubquerys: ");
                selectSubquerys.treePrint(depth+1);
            }

            printLabel(depth,"fromList: ");

            if(fromList!=null){
                fromList.treePrint(depth+1);
            }

            if(whereClause!=null){
                printLabel(depth,"whereClause: ");
                whereClause.treePrint(depth+1);
            }

            if((wherePredicates!=null) && !wherePredicates.isEmpty()){
                printLabel(depth,"wherePredicates: ");
                wherePredicates.treePrint(depth+1);
            }

            if(whereSubquerys!=null){
                printLabel(depth,"whereSubquerys: ");
                whereSubquerys.treePrint(depth+1);
            }

            if(groupByList!=null){
                printLabel(depth,"groupByList:");
                groupByList.treePrint(depth+1);
            }

            if(havingClause!=null){
                printLabel(depth,"havingClause:");
                havingClause.treePrint(depth+1);
            }

            if(orderByList!=null){
                printLabel(depth,"orderByList:");
                orderByList.treePrint(depth+1);
            }

            if(preJoinFL!=null){
                printLabel(depth,"preJoinFL: ");
                preJoinFL.treePrint(depth+1);
            }

            if(hasWindows()){
                printLabel(depth,"windows: ");
                windowDefinitionList.treePrint(depth+1);
            }
        }
    }

    /**
     * Return the fromList for this SelectNode.
     *
     * @return FromList    The fromList for this SelectNode.
     */
    @Override
    public FromList getFromList(){ return fromList; }

    /**
     * Find the corresponding column reference (as used in the select node) for the given column reference.
     * The select node here corresponds to a branch underneath a UnionNode
     * @param    cr        ColumnReference to find a match from the SELECT node
     * @return ColumnReference    ColumnReference pointing to the source of cr, if found
     */
    public ColumnReference findColumnReferenceInUnionSelect(ColumnReference cr) throws StandardException{
        ResultColumn rc = cr.getSource();
        if (rc == null)
            return null;

        //get the corresponding result column in the current SelectNode
        ResultColumn rcInSelect = resultColumns.elementAt(rc.getVirtualColumnId()-1);
        ValueNode mappedCR = rcInSelect.getExpression();
        if (!(mappedCR instanceof ColumnReference))
            return null;
         return (ColumnReference)mappedCR.getClone();
    }

    /**
     * Find colName in the result columns and return underlying columnReference.
     *
     * @param    colName        Name of the column
     * @return ColumnReference    ColumnReference to the column, if found
     */
    private ResultColumn getColumnInResult(String colName) throws StandardException{
        if (resultColumns != null) {
            // Loop through the result columns looking for a match
            int rclSize = resultColumns.size();
            for (int index = 0; index < rclSize; index++) {
                ResultColumn rc = resultColumns.elementAt(index);
                if (!(rc.getExpression() instanceof ColumnReference))
                    return null;

                ColumnReference crNode = (ColumnReference) rc.getExpression();

                if (crNode.columnName.equals(colName))
                    return rc;
            }
        }
        return null;
    }

    /**
     * Return the whereClause for this SelectNode.
     *
     * @return ValueNode    The whereClause for this SelectNode.
     */
    public ValueNode getWhereClause(){ return whereClause; }

    public void setWhereClause(ValueNode whereClause) {
        this.whereClause = whereClause;
    }

    public void setOriginalWhereClause(ValueNode originalWhereClause) {
        this.originalWhereClause = originalWhereClause;
    }

    /**
     * Return the wherePredicates for this SelectNode.
     *
     * @return PredicateList    The wherePredicates for this SelectNode.
     */
    public PredicateList getWherePredicates(){ return wherePredicates; }

    /**
     * Return the selectSubquerys for this SelectNode.
     *
     * @return SubqueryList    The selectSubquerys for this SelectNode.
     */
    public SubqueryList getSelectSubquerys(){ return selectSubquerys; }

    /**
     * Return the whereSubquerys for this SelectNode.
     *
     * @return SubqueryList    The whereSubquerys for this SelectNode.
     */
    public SubqueryList getWhereSubquerys(){ return whereSubquerys; }

    public SubqueryList getHavingSubquerys() {return havingSubquerys; }
    /**
     * Bind the tables in this SelectNode.  This includes getting their
     * TableDescriptors from the DataDictionary and numbering the FromTables.
     * NOTE: Because this node represents the top of a new query block, we bind
     * both the non VTI and VTI tables under this node in this method call.
     *
     * @param dataDictionary The DataDictionary to use for binding
     * @param fromListParam  FromList to use/append to.
     * @throws StandardException Thrown on error
     * @return ResultSetNode
     */
    @Override
    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
                                          FromList fromListParam) throws StandardException{
        int fromListSize=fromList.size();

        wherePredicates=(PredicateList)getNodeFactory().getNode( C_NodeTypes.PREDICATE_LIST, getContextManager());
        preJoinFL=(FromList)getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());

        /* Set the nesting level in the fromList */
        if(fromListParam.isEmpty()){
            nestingLevel=0;
        }else{
            nestingLevel=((FromTable)fromListParam.elementAt(0)).getLevel()+1;
        }
        fromList.setLevel(nestingLevel);

        /* Splice a clone of our FromList on to the beginning of fromListParam, before binding
         * the tables, for correlated column resolution in VTIs.
         */
        for(int index=0;index<fromListSize;index++){
            fromListParam.insertElementAt(fromList.elementAt(index),0);
        }

        // Now bind our from list
        fromList.bindTables(dataDictionary,fromListParam);

        /* Restore fromListParam */
        for(int index=0;index<fromListSize;index++){
            fromListParam.removeElementAt(0);
        }
        return this;
    }

    /**
     * Bind the expressions in this SelectNode.  This means binding the
     * sub-expressions, as well as figuring out what the return type is
     * for each expression.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindExpressions(FromList fromListParam) throws StandardException{
        int fromListParamSize=fromListParam.size();
        int fromListSize=fromList.size();

        assert resultColumns!=null: "ResultColumns is unexpectedly null!";

        /* NOTE - a lot of this code would be common to bindTargetExpression(),
         * so we use a private boolean to share the code instead of duplicating
         * it.  bindTargetExpression() is responsible for toggling the boolean.
         */
        if(!bindTargetListOnly){
            /* Bind the expressions in FromSubquerys, JoinNodes, etc. */
            fromList.bindExpressions(fromListParam);
        }

        selectSubquerys=(SubqueryList)getNodeFactory().getNode(
                C_NodeTypes.SUBQUERY_LIST,
                getContextManager());
        selectAggregates=new ArrayList<>();

        /* Splice our FromList on to the beginning of fromListParam, before binding
         * the expressions, for correlated column resolution.
         */
        for(int index=0;index<fromListSize;index++){
            fromListParam.insertElementAt(fromList.elementAt(index),index);
        }

        // In preparation for resolving window references in expressions, we
        // make the FromList carry the set of explicit window definitions.
        //
        // E.g. "select row_number () from r, .. from t window r as ()"
        //
        // Here the expression "row_number () from r" needs to be bound to r's
        // definition. Window functions can also in-line window specifications,
        // no resolution is necessary. See also
        // WindowFunctionNode.bindExpression.

        fromListParam.setWindows(windowDefinitionList);

        resultColumns.bindExpressions(fromListParam, selectSubquerys, selectAggregates);

        /* We're done if we're only binding the target list.
         * (After we restore the fromList, of course.)
         */
        if(bindTargetListOnly){
            for(int index=0;index<fromListSize;index++){
                fromListParam.removeElementAt(0);
            }
            return;
        }

        // Evaluate expressions with constant operands here to simplify the
        // query tree and to reduce the runtime cost. Do it before optimize()
        // since the simpler tree may have more accurate information for
        // the optimizer. (Example: The selectivity for 1=1 is estimated to
        // 0.1, whereas the actual selectivity is 1.0. In this step, 1=1 will
        // be rewritten to TRUE, which is known by the optimizer to have
        // selectivity 1.0.)
        // This is also done before predicate simplification to enable
        // more predicates to be pruned away.
        Visitor constantExpressionVisitor =
                new ConstantExpressionVisitor(SelectNode.class);
        if (whereClause != null)
            whereClause = (ValueNode)whereClause.accept(constantExpressionVisitor);
        if (havingClause != null)
            havingClause = (ValueNode)havingClause.accept(constantExpressionVisitor);

        // Perform predicate simplification.  Currently only
        // simple rewrites involving boolean TRUE/FALSE are done, such as:
        // TRUE AND col1 IN (1,2,3)  ==>  col1 IN (1,2,3)
        // FALSE OR col1 = 1         ==>  col1 = 1
        //
        // Predicate simplification is done before binding because
        // Subqueries and aggregates in the WHERE clause and HAVING
        // clause get added to the whereSubquerys, whereAggregates,
        // havingSubquerys and havingAggregates lists during binding.
        // If the predicates containing those SubqueryNodes and
        // AggregateNodes are subsequently removed from the WHERE or
        // HAVING clause, query compilation will fail.
        // Nested SelectNodes will not be scanned by this Visitor.
        if (!getCompilerContext().getDisablePredicateSimplification()) {
            Visitor predSimplVisitor =
                new PredicateSimplificationVisitor(fromListParam,
                                                   SelectNode.class);
            if (whereClause != null)
                whereClause = (ValueNode)whereClause.accept(predSimplVisitor);
            if (havingClause != null)
                havingClause = (ValueNode)havingClause.accept(predSimplVisitor);
        }

        whereAggregates=new LinkedList<>();
        whereSubquerys=(SubqueryList)getNodeFactory().getNode( C_NodeTypes.SUBQUERY_LIST, getContextManager());

        CompilerContext cc=getCompilerContext();

        if(whereClause!=null){
            cc.pushCurrentPrivType(Authorizer.SELECT_PRIV);

            int previousReliability=orReliability(CompilerContext.WHERE_CLAUSE_RESTRICTION);
            whereClause=whereClause.bindExpression(fromListParam, whereSubquerys, whereAggregates);
            cc.setReliability(previousReliability);

            /* RESOLVE - Temporarily disable aggregates in the HAVING clause.
            ** (We may remove them in the parser anyway.)
            ** RESOLVE - Disable aggregates in the WHERE clause.  Someday
            ** Aggregates will be allowed iff they are in a subquery
            ** of the having clause and they correlate to an outer
            ** query block.  For now, aggregates are not supported
            ** in the WHERE clause at all.
            ** Note: a similar check is made in JoinNode.
            */
            if(!whereAggregates.isEmpty()){
                throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_WHERE_CLAUSE);
            }

            /* If whereClause is a parameter, (where ?/where -?/where +?), then we should catch it and throw exception
             */
            if(whereClause.isParameterNode())
                throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_WHERE_CLAUSE,"PARAMETER");

            whereClause=whereClause.checkIsBoolean();
            getCompilerContext().popCurrentPrivType();

            checkNoWindowFunctions(whereClause,"WHERE");
            checkNoGroupingFunctions(whereClause, "WHERE");
        }

        if(havingClause!=null){
            int previousReliability=orReliability(CompilerContext.HAVING_CLAUSE_RESTRICTION);

            havingAggregates=new LinkedList<>();
            havingSubquerys=(SubqueryList)getNodeFactory().getNode( C_NodeTypes.SUBQUERY_LIST, getContextManager());
            havingClause.bindExpression(fromListParam,havingSubquerys,havingAggregates);
            havingClause=havingClause.checkIsBoolean();
            checkNoWindowFunctions(havingClause,"HAVING");

            cc.setReliability(previousReliability);
        }

        /* Restore fromList */
        for(int index=0;index<fromListSize;index++){
            fromListParam.removeElementAt(0);
        }// those items

        if(SanityManager.DEBUG){
            SanityManager.ASSERT(fromListParam.size()==fromListParamSize,
                    "fromListParam.size() = "+fromListParam.size()+
                            ", expected to be restored to "+fromListParamSize);
            SanityManager.ASSERT(fromList.size()==fromListSize,
                    "fromList.size() = "+fromList.size()+
                            ", expected to be restored to "+fromListSize);
        }

        /* If query is grouped, bind the group by list. */
        if(groupByList!=null){
            List<AggregateNode> gbAggregateVector=new LinkedList<>();

            groupByList.bindAndPullGroupByColumns(this, gbAggregateVector);

            /*
            ** There should be no aggregates in the Group By list.
            ** We don't expect any, but just to be on the safe side
            ** we will check under sanity.
            */
            assert gbAggregateVector.isEmpty() : "Unexpected Aggregate vector generated by Group By clause";

            checkNoWindowFunctions(groupByList,"GROUP BY");
        }

        // Bind the window node - partition and orderby
        if(hasWindows()){
            windowDefinitionList.bind(this);
        }

        /* If ungrouped query with aggregates in SELECT list, verify
         * that all result columns are valid aggregate expressions -
         * no column references outside of an aggregate.
         * If grouped query with aggregates in SELECT list, verify that all
         * result columns are either grouping expressions or valid
         * grouped aggregate expressions - the only column references
         * allowed outside of an aggregate are columns in expressions in
         * the group by list.
         */
        if((groupByList!=null || !selectAggregates.isEmpty())){
            VerifyAggregateExpressionsVisitor visitor= new VerifyAggregateExpressionsVisitor(groupByList);
            resultColumns.accept(visitor);
        } else {
            // non aggregate query should not contain GROUPING function
            checkNoGroupingFunctions(resultColumns, "SELECT");
        }
    }

    /**
     * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
     * sub-expressions, as well as figuring out what the return type is for
     * each expression.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindExpressionsWithTables(FromList fromListParam) throws StandardException{
        /* We have tables, so simply call bindExpressions() */
        bindExpressions(fromListParam);
    }

    /**
     * Bind the expressions in the target list.  This means binding the
     * sub-expressions, as well as figuring out what the return type is
     * for each expression.  This is useful for EXISTS subqueries, where we
     * need to validate the target list before blowing it away and replacing
     * it with a SELECT true.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindTargetExpressions(FromList fromListParam, boolean checkFromSubquery)
            throws StandardException{
        if (checkFromSubquery) {
        /*
         * With a FromSubquery in the FromList we cannot bind target expressions
         * at this level (DERBY-3321)
         */
            CollectNodesVisitor cnv = new CollectNodesVisitor(FromSubquery.class, FromSubquery.class);
            fromList.accept(cnv);
            bindTargetListOnly = cnv.getList().isEmpty();
        } else
            bindTargetListOnly = true;
        bindExpressions(fromListParam);
        bindTargetListOnly=false;
    }

    /**
     * Bind the result columns of this ResultSetNode when there is no
     * base table to bind them to.  This is useful for SELECT statements,
     * where the result columns get their types from the expressions that
     * live under them.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindResultColumns(FromList fromListParam) throws StandardException{
        /* We first bind the resultColumns for any FromTable which
         * needs its own binding, such as JoinNodes.
         * We pass through the fromListParam without adding our fromList
         * to it, since the elements in our fromList can only be correlated
         * with outer query blocks.
         */
        fromList.bindResultColumns(fromListParam);
        super.bindResultColumns(fromListParam);

        // Find the group-by-rollup columns in the resultset and mark it nullable
        if (groupByList != null && groupByList.isRollup()) {
            for (int i = 0; i < groupByList.size(); i++) {
                String col = groupByList.getGroupByColumn(i).getColumnExpression().getColumnName();
                ResultColumn rc = getColumnInResult(col);
                if (rc != null)
                    rc.setNullability(true);
            }
        }

        /* Only 1012 elements allowed in select list */
        if(resultColumns.size()>Limits.DB2_MAX_ELEMENTS_IN_SELECT_LIST){
            throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
        }

        // DERBY-4407: A derived table must have at least one column.
        if(resultColumns.isEmpty()){
            throw StandardException.newException(
                    SQLState.LANG_EMPTY_COLUMN_LIST);
        }
    }

    /**
     * Bind the result columns for this ResultSetNode to a base table.
     * This is useful for INSERT and UPDATE statements, where the
     * result columns get their types from the table being updated or
     * inserted into.
     * If a result column list is specified, then the verification that the
     * result column list does not contain any duplicates will be done when
     * binding them by name.
     *
     * @param targetTableDescriptor The TableDescriptor for the table being
     *                              updated or inserted into
     * @param targetColumnList      For INSERT statements, the user
     *                              does not have to supply column
     *                              names (for example, "insert into t
     *                              values (1,2,3)".  When this
     *                              parameter is null, it means that
     *                              the user did not supply column
     *                              names, and so the binding should
     *                              be done based on order.  When it
     *                              is not null, it means do the binding
     *                              by name, not position.
     * @param statement             Calling DMLStatementNode (Insert or Update)
     * @param fromListParam         FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindResultColumns(TableDescriptor targetTableDescriptor,
                                  FromVTI targetVTI,
                                  ResultColumnList targetColumnList,
                                  DMLStatementNode statement,
                                  FromList fromListParam) throws StandardException{
        /* We first bind the resultColumns for any FromTable which
         * needs its own binding, such as JoinNodes.
         * We pass through the fromListParam without adding our fromList
         * to it, since the elements in our fromList can only be correlated
         * with outer query blocks.
         */
        fromList.bindResultColumns(fromListParam);
        super.bindResultColumns(targetTableDescriptor,targetVTI,targetColumnList,statement,fromListParam);
    }

    /**
     * Verify that a SELECT * is valid for this type of subquery.
     *
     * @param outerFromList The FromList from the outer query block(s)
     * @param subqueryType  The subquery type
     * @throws StandardException Thrown on error
     */
    @Override
    public void verifySelectStarSubquery(FromList outerFromList,int subqueryType) throws StandardException{
        for(int i=0;i<resultColumns.size();i++){
            if(!(resultColumns.elementAt(i) instanceof AllResultColumn)){
                continue;
            }

            /* Select * currently only valid for EXISTS/NOT EXISTS.  NOT EXISTS
             * does not appear prior to preprocessing.
             */
            if(subqueryType!=SubqueryNode.EXISTS_SUBQUERY){
                throw StandardException.newException(SQLState.LANG_CANT_SELECT_STAR_SUBQUERY);
            }

            /* If the AllResultColumn is qualified, then we have to verify that
             * the qualification is a valid exposed name.  NOTE: The exposed
             * name can come from an outer query block.
             */
            String fullTableName= ((AllResultColumn)resultColumns.elementAt(i)).getFullTableName();

            if(fullTableName!=null){
                FromTable outerFullTable=outerFromList.getFromTableByName(fullTableName,null,true);
                FromTable fromTable=fromList.getFromTableByName(fullTableName,null,true);
                if(fromTable==null && outerFullTable==null){
                    outerFullTable=outerFromList.getFromTableByName(fullTableName,null,false);
                    fromTable=fromList.getFromTableByName(fullTableName,null,false);
                    if(fromTable==null && outerFullTable==null){
                        throw StandardException.newException(SQLState.LANG_EXPOSED_NAME_NOT_FOUND, fullTableName);
                    }
                }
            }
        }
    }

    /**
     * Check for (and reject) ? parameters directly under the ResultColumns.
     * This is done for SELECT statements.
     *
     * @throws StandardException Thrown if a ? parameter found
     *                           directly under a ResultColumn
     */
    @Override
    public void rejectParameters() throws StandardException{
        super.rejectParameters();
        fromList.rejectParameters();
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
     * @param numTables The number of tables in the DML Statement
     * @param gbl       The outer group by list, if any
     * @param fl        The from list, if any
     * @return The generated ProjectRestrictNode atop the original FromTable.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode preprocess(int numTables,GroupByList gbl,FromList fl) throws StandardException{
        ResultSetNode newTop=this;

        /* Put the expression trees in conjunctive normal form.
         * NOTE - This needs to occur before we preprocess the subqueries
         * because the subquery transformations assume that any subquery operator
         * negation has already occurred.
         */
        whereClause=normExpressions(whereClause);
        // DERBY-3257. We need to normalize the having clause as well, because
        // preProcess expects CNF.
        havingClause=normExpressions(havingClause);

        /**
         * This method determines if (1) the query is a LOJ, and (2) if the LOJ is a candidate for
         * reordering (i.e., linearization).  The condition for LOJ linearization is:
         * 1. either LOJ or ROJ in the fromList, i.e., no INNER, NO FULL JOINs
         * 2. ON clause must be equality join between left and right operands and in CNF (i.e., AND is allowed)
         */
        boolean anyChange=fromList.LOJ_reorderable(numTables);
        if(anyChange){
            FromList afromList=(FromList)getNodeFactory().getNode(C_NodeTypes.FROM_LIST,
                    getNodeFactory().doJoinOrderOptimization(),
                    getContextManager());
            bindExpressions(afromList);
            // bindExpressions() does constant folding and predicate simplification for
            // where and having clause and get rid of the top AND node with 1=1,
            // the top AND node is always expected from the subsequent
            // logic, so we need to call normExpressions() again to normalize WHERE clause.
            whereClause = normExpressions(whereClause);
            havingClause=normExpressions(havingClause);
            fromList.bindResultColumns(afromList);
        }

        /* Preprocess the fromList.  For each FromTable, if it is a FromSubquery
         * then we will preprocess it, replacing the FromSubquery with a
         * ProjectRestrictNode. If it is a FromBaseTable, then we will generate
         * the ProjectRestrictNode above it.
         */
        fromList.preprocess(numTables,groupByList,whereClause);

        /* selectSubquerys is always allocated at bind() time */
        assert selectSubquerys!=null: "selectSubquerys is expected to be non-null";

        /* Preprocess the RCL after the from list so that
         * we can flatten/optimize any subqueries in the
         * select list.
         */
        resultColumns.preprocess(numTables, fromList,whereSubquerys, wherePredicates);

        /* Preprocess the expressions.  (This is necessary for subqueries.
         * This is also where we do tranformations such as for LIKE.)
         *
         * NOTE: We do this after preprocessing the fromList so that, for
         * quantified subqueries, the join expression with the outer
         * expression will be pushable (to be pushable, the ColumnReference
         * has to point to a VirtualColumnNode, and not to a BaseColumnNode).
         */
        if(whereClause!=null){
            // DERBY-3301
            // Mark subqueries that are part of the where clause as such so
            // that we can avoid flattening later, particularly for nested
            // WHERE EXISTS subqueries.
            if(whereSubquerys!=null){
                whereSubquerys.markWhereSubqueries();
            }
            whereClause.preprocess(numTables, fromList,whereSubquerys, wherePredicates);
        }

        /* Preprocess the group by list too. We need to compare
         * expressions in the group by list with the select list and we
         * can't rewrite one and not the other.
         */
        if(groupByList!=null){
            groupByList.preprocess(numTables,fromList,whereSubquerys,wherePredicates);
        }

        if(havingClause!=null){
            // DERBY-3257
            // Mark  subqueries that are part of the having clause as
            // such so we can avoid flattenning later. Having subqueries
            // cannot be flattened because we cannot currently handle
            // column references at the same source level.
            // DERBY-3257 required we normalize the having clause which
            // triggered flattening because SubqueryNode.underTopAndNode
            // became true after normalization.  We needed another way to
            // turn flattening off. Perhaps the long term solution is
            // to avoid this restriction all together but that was beyond
            // the scope of this bugfix.
            havingSubquerys.markHavingSubqueries();
            havingClause=havingClause.preprocess(numTables,fromList,havingSubquerys,wherePredicates);
        }

        /* Pull apart the expression trees */
        if(whereClause!=null){
            wherePredicates.pullExpressions(numTables,whereClause);
            whereClause=null;
        }

        /* RESOLVE - Where should we worry about expression pull up for
         * expensive predicates?
         */

        // Flatten any flattenable FromSubquerys or JoinNodes
        fromList.flattenFromTables(resultColumns, wherePredicates, whereSubquerys, groupByList, havingClause, numTables);

        if(wherePredicates!=null && !wherePredicates.isEmpty() && !fromList.isEmpty()){
            // Perform various forms of transitive closure on wherePredicates
            if(fromList.size()>1){
                performTransitiveClosure();
            }

            if(orderByList!=null){

                // When left outer join is flattened, its ON clause condition could be released to the WHERE clause but
                // with an outerJoinLevel > 0. These predicates cannot be used to eliminate order by columns
                PredicateList levelZeroPredicateList = (PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());
                for (int i = wherePredicates.size()-1; i >= 0 ; i--) {
                    Predicate pred = wherePredicates.elementAt(i);
                    if (pred.getOuterJoinLevel() == 0) {
                        levelZeroPredicateList.addOptPredicate(pred);
                    }
                }
                // Remove constant columns from order by list.  Constant
                // columns are ones that have equality comparisons with
                // constant expressions (e.g. x = 3)
                orderByList.removeConstantColumns(levelZeroPredicateList);
                /*
                ** It's possible for the order by list to shrink to nothing
                ** as a result of removing constant columns.  If this happens,
                ** get rid of the list entirely.
                */
                if(orderByList.isEmpty()){
                    orderByList=null;
                    resultColumns.removeOrderByColumns();
                }
                levelZeroPredicateList.removeAllPredicates();
            }
        }

        /**
         * Now that we have flatten all flattenable subqueries. For table in FromList, it is originally from the
         * Scalar subquery in the Select List, we want them to be joined last after all the other tables are joined.
         * These joins need to be done as outer join and also we need to enforce scalar subquery's "at most one row"
         * semantic check at run time, so they could be expensive.
         * Set their dependencyMap so that they can be joined last.
         */
        fromList.moveSSQAndSetDependencyMap(numTables);

        /* A valid group by without any aggregates or a having clause
         * is equivalent to a distinct without the group by.  We do the transformation
         * in order to simplify the group by code.
         * The conversion cannot be done if it is a rollup
         */
        if(groupByList!=null && !groupByList.isRollup() && havingClause==null && !hasAggregatesInSelectList() && whereAggregates.isEmpty()){
            isDistinct=true;
            groupByList=null;
            wasGroupBy=true;
            if (getCompilerContext().isProjectionPruningEnabled()) {
                /**
                 *  We need to mark all columns in RCL as referenced, as they are the columns where distinct will be
                 *  applied on
                 */
                resultColumns.setColumnReferences(true, true);
            }
        }

        /* Consider distinct elimination based on a uniqueness condition.
         * In order to do this:
         *    o  All top level ColumnReferences in the select list are
         *       from the same base table.  (select t1.c1, t2.c2 + t3.c3 is
         *       okay - t1 is the table of interest.)
         *  o  If the from list is a single table then the columns in the
         *       select list plus the columns in the where clause that are
         *       in = comparisons with constants or parameters must be a
         *       superset of any unique index.
         *  o  If the from list has multiple tables then at least 1 table
         *       meet the following - the set of columns in = comparisons
         *       with constants or parameters is a superset of any unique
         *       index on the table.  All of the other tables must meet
         *       the following - the set of columns in = comparisons with
         *       constants, parameters or join columns is a superset of
         *       any unique index on the table.  If the table from which
         *       the columns in the select list are coming from is in this
         *     later group then the rule for it is to also include
         *     the columns in the select list in the set of columns that
         *     needs to be a superset of the unique index.  Whew!
         */
        if(isDistinct && groupByList==null){
            int distinctTable=resultColumns.allTopCRsFromSameTable();

            if(distinctTable!=-1){
                if(fromList.returnsAtMostSingleRow(resultColumns, whereClause,wherePredicates )){
                    isDistinct=false;
                }
            }

            /* If we were unable to eliminate the distinct and we have
             * an order by then we can consider eliminating the sort for the
             * order by.  All of the columns in the order by list must
             * be ascending in order to do this.  There are 2 cases:
             *    o    The order by list is an in order prefix of the columns
             *        in the select list.  In this case the output of the
             *        sort from the distinct will be in the right order
             *        so we simply eliminate the order by list.
             *    o    The order by list is a subset of the columns in the
             *        the select list.  In this case we need to reorder the
             *        columns in the select list so that the ordering columns
             *        are an in order prefix of the select list and put a PRN
             *        above the select so that the shape of the result set
             *        is as expected.
             */

//            if(isDistinct && orderByList!=null && orderByList.allAscending()){
                /* Order by list currently restricted to columns in select
                 * list, so we will always eliminate the order by here.
                 */
//                if (orderByList.isInOrderPrefix(resultColumns))
//                {
//                    orderByList = null;
//                }
//                else
//                {
                /* Order by list is not an in order prefix of the select list
                 * so we must reorder the columns in the the select list to
                 * match the order by list and generate the PRN above us to
                 * preserve the expected order.
                 */
//                newTop=genProjectRestrictForReordering();
//                orderByList.resetToSourceRCs();
//                resultColumns=orderByList.reorderRCL(resultColumns);
//                newTop.getResultColumns().removeOrderByColumns();
//                orderByList=null;
//                }
//                orderByAndDistinctMerged=true;
//            }
        }

        /*
         * Push predicates that are pushable.
         *
         * NOTE: We pass the wherePredicates down to the new PRNs here,
         * so they can pull any clauses and possibily push them down further.
         * NOTE: We wait until all of the FromTables have been preprocessed
         * until we attempt to push down predicates, because we cannot push down
         * a predicate if the immediate source of any of its column references
         * is not a ColumnReference or a VirtualColumnNode.
         */
        fromList.pushPredicates(wherePredicates);

        /* Set up the referenced table map */
        referencedTableMap=new JBitSet(numTables);
        int flSize=fromList.size();
        for(int index=0;index<flSize;index++){
            referencedTableMap.or(((FromTable)fromList.elementAt(index)).getReferencedTableMap());
        }

        /* Copy the referenced table map to the new tree top, if necessary */
        /* the code that update newTop above has been commented out, so this code has become obselete
        if(newTop!=this){
            newTop.setReferencedTableMap((JBitSet)referencedTableMap.clone());
        }
        */


        if(orderByList!=null){

            // Collect window function calls and in-lined window definitions
            // contained in them from the orderByList.

            CollectNodesVisitor cnvw= new CollectNodesVisitor(WindowFunctionNode.class);
            orderByList.accept(cnvw);
            Vector wfcInOrderBy=cnvw.getList();

            for(int i=0;i<wfcInOrderBy.size();i++){
                WindowFunctionNode wfn= (WindowFunctionNode)wfcInOrderBy.elementAt(i);
                windowFuncCalls.add(wfn);

                assert wfn.getWindow() instanceof WindowDefinitionNode: "a window reference should be bound already";
                // Window function call contains an inline definition, add
                // it to our list of windowDefinitionList.
                windowDefinitionList = addInlinedWindowDefinition(windowDefinitionList, wfn);
            }
        }

        return newTop;
    }

    /**
     * Add a new predicate to the list.  This is useful when doing subquery
     * transformations, when we build a new predicate with the left side of
     * the subquery operator and the subquery's result column.
     *
     * @param predicate The predicate to add
     * @return ResultSetNode    The new top of the tree.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode addNewPredicate(Predicate predicate) throws StandardException{
        wherePredicates.addPredicate(predicate);
        return this;
    }

    /**
     * Evaluate whether or not the subquery in a FromSubquery is flattenable.
     * Currently, a FSqry is flattenable if all of the following are true:
     * o  Subquery is a SelectNode. (ie, not a RowResultSetNode or a UnionNode)
     * o  It contains a single table in its FROM list.
     * o  It contains no subqueries in the SELECT list.
     * o  It does not contain a group by or having clause
     * o  It does not contain aggregates.
     * o  It is not a DISTINCT.
     * o  It does not have an ORDER BY clause (pushed from FromSubquery).
     *
     * @param fromList The outer from list
     * @return boolean    Whether or not the FromSubquery is flattenable.
     */
    @Override
    public boolean flattenableInFromSubquery(FromList fromList){
        if(isDistinct){
            return false;
        }
        if(this.fromList.size()>1){
            return false;
        }

        /* Don't flatten (at least for now) if selectNode's SELECT list contains a subquery */
        if((selectSubquerys!=null) && (!selectSubquerys.isEmpty())){
            return false;
        }

        /* Don't flatten if selectNode contains a group by or having clause */
        if((groupByList!=null) || (havingClause!=null)){
            return false;
        }

        /* Don't flatten if select list contains something that isn't cloneable.
         */
        if(!resultColumns.isCloneable()){
            return false;
        }

        /* Don't flatten if selectNode contains an aggregate */
        if((selectAggregates!=null) && (!selectAggregates.isEmpty())){
            return false;
        }

        /* Don't flatten if selectNode now has an order by */
        if((orderByList!=null) && (!orderByList.isEmpty())){
            return false;
        }

        /* Don't flatten if selectNode has OFFSET or FETCH */
        return !((offset!=null) || (fetchFirst!=null));
    }

    /**
     * Replace this SelectNode with a ProjectRestrictNode,
     * since it has served its purpose.
     *
     * @param origFromListSize The size of the original FROM list, before
     *                         generation of join tree.
     * @return ResultSetNode    new ResultSetNode atop the query tree.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode genProjectRestrict(int origFromListSize) throws StandardException{
        boolean eliminateSort=false;
        ResultSetNode prnRSN;

        prnRSN=(ResultSetNode)getNodeFactory().getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                fromList.elementAt(0),    /* Child ResultSet */
                resultColumns,        /* Projection */
                whereClause,            /* Restriction */
                wherePredicates,/* Restriction as PredicateList */
                selectSubquerys,/* Subquerys in Projection */
                whereSubquerys,    /* Subquerys in Restriction */
                null,
                getContextManager());

        if (getCompilerContext().isProjectionPruningEnabled()) {
            int numPruned = prnRSN.getResultColumns().doProjection(false);
            if (numPruned > 0) {
                // we need to adjust the columnPosition in orderbylist and groupByList
                if (orderByList != null) {
                    for (int i = 0; i < orderByList.size(); i++) {
                        OrderByColumn oCol = (OrderByColumn) orderByList.elementAt(i);
                        oCol.columnPosition = oCol.getResultColumn().getVirtualColumnId();
                    }
                }
            }
        }

        /*
        ** If we have aggregates OR a select list we want
        ** to generate a GroupByNode.  In the case of a
        ** scalar aggregate we have no grouping columns.
        **
        ** JRESOLVE: what about correlated aggregates from another
        ** block.
        */
        if(((selectAggregates!=null) && (!selectAggregates.isEmpty())) || (groupByList!=null)){

            List<AggregateNode> aggs=selectAggregates;
            if(havingAggregates!=null && !havingAggregates.isEmpty()){
                if(selectAggregates!=null)
                    havingAggregates.addAll(selectAggregates);
                aggs=havingAggregates;
            }
            GroupByNode gbn=(GroupByNode)getNodeFactory().getNode(
                    C_NodeTypes.GROUP_BY_NODE,
                    prnRSN,
                    groupByList,
                    aggs,
                    havingClause,
                    havingSubquerys,
                    null,
                    nestingLevel,
                    getContextManager());
            gbn.considerPostOptimizeOptimizations(originalWhereClause!=null);
            // JL-TODO Interesting
            CostEstimate ce = gbn.estimateCost(null,null, optimizer.getOptimizedCost(), optimizer,null);
            gbn.assignCostEstimate(ce);
            prnRSN=gbn.getParent();
            eliminateSort=gbn.getIsInSortedOrder();
        }

        // Pull up rowId predicates that are not start or top keys
        pullRowIdPredicates((ProjectRestrictNode)prnRSN);
        if(hasWindows()){
            // Now we add a window result set wrapped in a PRN on top of what we currently have.
            for (WindowNode windowDefinition : windowDefinitionList) {
                WindowResultSetNode wrsn =
                        (WindowResultSetNode) getNodeFactory().getNode(
                                C_NodeTypes.WINDOW_RESULTSET_NODE,
                                prnRSN,
                                windowDefinition,
                                null,   // table properties
                                nestingLevel,
                                getContextManager());

                prnRSN = wrsn.processWindowDefinition();
                // TODO-JL NOT OPTIMAL
                wrsn.assignCostEstimate(optimizer.getOptimizedCost());
            }
        }

        // if it is distinct, that must also be taken care of.
        if(isDistinct){
            // We first verify that a distinct is valid on the
            // RCL.
            resultColumns.verifyAllOrderable();

            /* See if we can push duplicate elimination into the store
             * via a hash scan.  This is possible iff:
             *    o  A single table query
             *    o  We haven't merged the order by and distinct sorts.
             *       (Results do not have to be in a particular order.)
             *    o  All entries in the select's RCL are ColumnReferences.
             *    o  No predicates (This is because we currently do not
             *       differentiate between columns referenced in the select
             *       list and columns referenced in other clauses.  In other
             *       words, the store will do duplicate elimination based on
             *       all referenced columns.)
             *       RESOLVE - We can change this to be all referenced columns
             *       have to be in the select list.  In that case, we need to
             *       refine which predicates are allowed.  Basically, all predicates
             *       must have been pushed down to the index/table scan.(If we make
             *       this change, then we need to verify that non of the columns in
             *       the predicates are correlated columns.)
             *    o  NOTE: The implementation of isPossibleDistinctScan() will return
             *       false if there is an IndexRowToBaseRow above the
             *       FromBaseTable.  This is because all of a table's columns must come
             *       from the same conglomerate in order to get consistent data.
             */
            boolean distinctScanPossible=false;
            if(origFromListSize==1 && !orderByAndDistinctMerged){
                boolean simpleColumns=true;
                Set<BaseColumnNode> distinctColumns=new HashSet<>();
                int size=resultColumns.size();
                for(int i=1;i<=size;i++){
                    BaseColumnNode bc=resultColumns.getResultColumn(i).getBaseColumnNode();
                    if(bc==null){
                        simpleColumns=false;
                        break;
                    }
                    distinctColumns.add(bc);
                }
                if(simpleColumns && prnRSN.isPossibleDistinctScan(distinctColumns)){
                    prnRSN.markForDistinctScan();
                    distinctScanPossible=true;
                }
            }

            if(!distinctScanPossible){
                /* We can't do a distinct scan. Determine if we can filter out
                 * duplicates without a sorter.
                 */
                boolean inSortedOrder=isOrderedResult(resultColumns,prnRSN,!(orderByAndDistinctMerged));
                prnRSN=(ResultSetNode)getNodeFactory().getNode(
                        C_NodeTypes.DISTINCT_NODE,
                        prnRSN,
                        inSortedOrder,
                        null,
                        getContextManager());
                // TODO NOT-OPTIMAL
                // Remember whether or not we can eliminate the sort.
                eliminateSort=eliminateSort || inSortedOrder;
                ((DistinctNode)prnRSN).estimateCost(null, null, null, optimizer, null);
            }
        }

        /* Generate the OrderByNode if a sort is still required for
         * the order by.
         */

        if(orderByList!=null){
            // Need to remove sort reduction if you are aggregating (hash)
            if(orderByList.isSortNeeded()
                    || (((selectAggregates!=null) && (!selectAggregates.isEmpty())) || (groupByList!=null))){
                prnRSN=(ResultSetNode)getNodeFactory().getNode(
                        C_NodeTypes.ORDER_BY_NODE,
                        prnRSN,
                        orderByList,
                        null,
                        getContextManager());
                // TODO JL NOT OPTIMAL
//                prnRSN.costEstimate=optimizer.getOptimizedCost().cloneMe();
            }

            // There may be columns added to the select projection list
            // a query like:
            // select a, b from t group by a,b order by a+b
            // the expr a+b is added to the select list.
            int orderBySelect=this.getResultColumns().getOrderBySelect();
            if(orderBySelect>0){
                // Keep the same RCL on top, since there may be references to
                // its result columns above us, i.e. in this query:
                //
                // select sum(j),i from t group by i having i
                //             in (select i from t order by j)
                //
                ResultColumnList topList=prnRSN.getResultColumns();
                ResultColumnList newSelectList=topList.copyListAndObjects();
                prnRSN.setResultColumns(newSelectList);

                topList.removeOrderByColumns();
                topList.genVirtualColumnNodes(prnRSN,newSelectList);
                prnRSN=(ResultSetNode)getNodeFactory().getNode(
                        C_NodeTypes.PROJECT_RESTRICT_NODE,
                        prnRSN,
                        topList,
                        null,
                        null,
                        null,
                        null,
                        null,
                        getContextManager());
            }
        }

        if(offset!=null || fetchFirst!=null){
            // Keep the same RCL on top, since there may be references to
            // its result columns above us.
            ResultColumnList topList=prnRSN.getResultColumns();
            ResultColumnList newSelectList=topList.copyListAndObjects();
            prnRSN.setResultColumns(newSelectList);
            topList.genVirtualColumnNodes(prnRSN,newSelectList);
            prnRSN=(ResultSetNode)getNodeFactory().getNode(
                    C_NodeTypes.ROW_COUNT_NODE,
                    prnRSN,
                    topList,
                    offset,
                    fetchFirst,
                    hasJDBClimitClause,
                    getContextManager());
        }


        if(wasGroupBy && resultColumns.numGeneratedColumnsForGroupBy()>0 && ! hasWindows()) {
            // windows handling already added a PRN which obviates this

            // This case takes care of columns generated for group by's which
            // will need to be removed from the final projection. Note that the
            // GroupByNode does remove generated columns but in certain cases
            // we dispense with a group by and replace it with a distinct instead.
            // So in a query like:
            // select c1 from t group by c1, c2
            // we would have added c2 to the projection list which will have to be
            // projected out.
            //

            // Keep the same RCL on top, since there may be
            // references to its result columns above us, e.g. in this query:
            //
            // select sum(j),i from t group by i having i
            //             in (select i from t group by i,j )
            //
            ResultColumnList topList=prnRSN.getResultColumns();
            ResultColumnList newSelectList=topList.copyListAndObjects();
            prnRSN.setResultColumns(newSelectList);

            topList.removeGeneratedGroupingColumns();
            topList.genVirtualColumnNodes(prnRSN,newSelectList);
            prnRSN=(ResultSetNode)getNodeFactory().getNode(
                    C_NodeTypes.PROJECT_RESTRICT_NODE,
                    prnRSN,
                    topList,
                    null,
                    null,
                    null,
                    null,
                    null,
                    getContextManager());
        }

        if(!(orderByList!=null && orderByList.isSortNeeded()) && orderByQuery){
            // Remember whether or not we can eliminate the sort.
            eliminateSort=true;
        }

        /* If we were able to eliminate the sort during optimization then
         * we must tell the underlying tree.  At minimum, this means no
         * group fetch on an index under an IndexRowToBaseRow since that
         * that could lead to incorrect results.  (Bug 2347.)
         */
        if(eliminateSort){
            prnRSN.adjustForSortElimination(orderByList);
        }

        /* Set the cost of this node in the generated node */
//        if(prnRSN.costEstimate==null)
//            prnRSN.costEstimate=costEstimate.cloneMe();

        return prnRSN;
    }

    /**
     * Ensure that the top of the RSN tree has a PredicateList.
     *
     * @param numTables The number of tables in the query.
     * @return ResultSetNode    A RSN tree with a node which has a PredicateList on top.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode ensurePredicateList(int numTables) throws StandardException{
        return this;
    }

    /* check if specialMaxScan is possible
       criteria:
       1. This is for control path execution
       2. The select has no constraints, that is originalWhereClause!=null
       3. The select contains only one base table;
       4. there is only one min/max aggregate with no group by
       5. the parameter of the aggregate is a simple column reference, not any complex expression
    */
    private boolean setUpSpecialMaxScanIfPossible(boolean forSpark) {
        if (forSpark)
            return false;

        if (originalWhereClause != null)
            return false;

        if (fromList.size() > 1)
            return false;

        if (!(fromList.elementAt(0) instanceof ProjectRestrictNode) ||
             !(((ProjectRestrictNode)fromList.elementAt(0)).getChildResult() instanceof FromBaseTable))
            return false;

        FromBaseTable table = (FromBaseTable)((ProjectRestrictNode)fromList.elementAt(0)).getChildResult();

        if (groupByList != null)
            return false;

        List<AggregateNode> aggs= new ArrayList<>();
        if (selectAggregates != null && !selectAggregates.isEmpty())
            aggs.addAll(selectAggregates);
        if (havingAggregates != null && !havingAggregates.isEmpty())
                aggs.addAll(havingAggregates);

        if (aggs.size() != 1)
            return false;

        AggregateNode an=aggs.get(0);
        AggregateDefinition ad=an.getAggregateDefinition();
        if (!(ad instanceof MaxMinAggregateDefinition))
            return false;

        if (!(an.getOperand() instanceof ColumnReference))
            return false;

        ColumnReference cr = (ColumnReference)an.getOperand();
        if (cr.getTableNumber() != table.tableNumber)
            return false;

        // If all the above criteria meet, then set the Aggregate in the FromBaseTable node.
        // We need to check if the column reference in the aggregate node is a leading index/PK column during costing,
        // so that we can determine if specialMaxScan can be used or not
        table.setAggregateForSpecialMaxScan(an);
        return true;
    }

    /**
     * Optimize this SelectNode.  This means choosing the best access path
     * for each table, among other things.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicateList  The predicate list to optimize against
     * @param outerRows      The number of outer joining rows
     * @param forSpark
     * @throws StandardException Thrown on error
     * @return ResultSetNode    The top of the optimized tree
     */
    @Override
    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicateList,
                                  double outerRows,
                                  boolean forSpark) throws StandardException{
        /* Optimize any subquerys before optimizing the underlying result set */

        /* selectSubquerys is always allocated at bind() time */
        assert selectSubquerys != null: "selectSubquerys is expected to be non-null";

        /* If this select node is the child of an outer node that is
         * being optimized, we can get here multiple times (once for
         * every permutation that is done for the outer node).  With
         * DERBY-805, we can add optimizable predicates to the WHERE
         * list as part of this method; thus, before proceeding we
         * need go through and remove any opt predicates that we added
         * to our WHERE list the last time we were here; if we don't
         * do that, we'll end up with the same predicates in our
         * WHERE list multiple times, which can lead to incorrect
         * optimization.
         */
        if(wherePredicates!=null){
            // Iterate backwards because we might be deleting entries.
            for(int i=wherePredicates.size()-1;i>=0;i--){
                if(wherePredicates.elementAt(i).isScopedForPush())
                    wherePredicates.removeOptPredicate(i);
            }
        }

        /* With DERBY-805 we take any optimizable predicates that
         * were pushed into this node and we add them to the list of
         * predicates that we pass to the optimizer, thus allowing
         * the optimizer to use them when choosing an access path
         * for this SELECT node.  We do that by adding the predicates
         * to our WHERE list, since the WHERE predicate list is what
         * we pass to the optimizer for this select node (see below).
         * We have to pass the WHERE list directly (as opposed to
         * passing a copy) because the optimizer is only created one
         * time; it then uses the list we pass it for the rest of the
         * optimization phase and finally for "modifyAccessPaths()".
         * Since the optimizer can update/modify the list based on the
         * WHERE predicates (such as by adding internal predicates or
         * by modifying the actual predicates themselves), we need
         * those changes to be applied to the WHERE list directly for
         * subsequent processing (esp. for modification of the access
         * path).  Note that by adding outer opt predicates directly
         * to the WHERE list, we're changing the semantics of this
         * SELECT node.  This is only temporary, though--once the
         * optimizer is done with all of its work, any predicates
         * that were pushed here will have been pushed even further
         * down and thus will have been removed from the WHERE list
         * (if it's not possible to push them further down, then they
         * shouldn't have made it this far to begin with).
         */
        if(predicateList!=null){
            if(wherePredicates==null){
                wherePredicates=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST, getContextManager());
            }

            Predicate pred;
            int sz=predicateList.size();
            for(int i=sz-1;i>=0;i--){
                // We can tell if a predicate was pushed into this select
                // node because it will have been "scoped" for this node
                // or for some result set below this one.
                pred=(Predicate)predicateList.getOptPredicate(i);
                if(pred.isScopedToSourceResultSet()){
                    // If we're pushing the predicate down here, we have to
                    // remove it from the predicate list of the node above
                    // this select, in order to keep in line with established
                    // push 'protocol'.
                    wherePredicates.addOptPredicate(pred);
                    predicateList.removeOptPredicate(pred);
                }
            }
        }
        if((orderByList!=null && orderByList.isSortNeeded()) &&
                (((selectAggregates!=null) && (!selectAggregates.isEmpty())) || (groupByList!=null))){
            // Preserve order by sort for aggregates with group by, otherwise
            // the optimizer will optimize away the sort.
            orderByList.setAlwaysSort();
        }

        /* Currently, SelfReferenceNode cannot be used as the right table of a nested loop join or broadcast join,
           so let all other tables in the fromList set dependency on this to ensure SelfReferenceNode to be planned first
         */
        FromTable selfReferenceNode = null;
        for (int i=0; i < fromList.size(); i++) {
            FromTable ft=(FromTable)fromList.elementAt(i);
            /* we only allow one selfReference in recursive quer for now */
            if (ft.getContainsSelfReference()) {
                selfReferenceNode = ft;
                break;
            }
        }
        if (selfReferenceNode != null) {
            for (int i=0; i < fromList.size(); i++) {
                FromTable ft=(FromTable)fromList.elementAt(i);
                if (ft != selfReferenceNode) {
                    ft.addToDependencyMap(selfReferenceNode.getReferencedTableMap());
                }
            }
        }

        setUpSpecialMaxScanIfPossible(forSpark);
        Optimizer optimizer = getOptimizer(fromList, wherePredicates, dataDictionary, orderByList);
        optimizer.setForSpark(forSpark);
        findBestPlan(optimizer, dataDictionary, outerRows);



        /* When we're done optimizing, any scoped predicates that
         * we pushed down the tree should now be sitting again
         * in our wherePredicates list.  Put those back in the
         * the list from which we received them, to allow them
         * to be "pulled" back up to where they came from.
         */
        if(wherePredicates!=null){
            Predicate pred;
            for(int i=wherePredicates.size()-1;i>=0;i--){
                pred=(Predicate)wherePredicates.getOptPredicate(i);
                if(pred.isScopedForPush()){
                    //predicateList is not null because wherePredicates wouldn't have anything if it were
                    //noinspection ConstantConditions
                    predicateList.addOptPredicate(pred);
                    wherePredicates.removeOptPredicate(pred);
                }
            }
        }

        selectSubquerys.optimize(dataDictionary,costEstimate.rowCount(), optimizer.isForSpark());

        if(whereSubquerys!=null && !whereSubquerys.isEmpty()){
            whereSubquerys.optimize(dataDictionary,costEstimate.rowCount(), optimizer.isForSpark());
        }

        if(havingSubquerys!=null && !havingSubquerys.isEmpty()){
            havingSubquerys.optimize(dataDictionary,costEstimate.rowCount(), optimizer.isForSpark());
        }

        return this;
    }

    private void findBestPlan(Optimizer optimizer, DataDictionary dataDictionary, double outerRows) throws StandardException {
        optimizer.setOuterRows(outerRows);

        // Aggregation with no GROUP BY always outputs one row.
        if ((selectAggregates != null && !selectAggregates.isEmpty() ||
                havingAggregates !=null  && !havingAggregates.isEmpty()) && hasNoGroupBy())
            optimizer.setSingleRow(true);

        while(optimizer.nextJoinOrder()){
            while(optimizer.getNextDecoratedPermutation()){
                optimizer.costPermutation();
            }
        }

        /*
         * DB-2001/DB-2877. At the end of this loop we need to verify
         * that we found a best plan for *our subtree*--otherwise, if we are a
         * subselect for a larger plan, the outer select will not detect that an error
         * should be thrown, and it will just fall into an infinite loop. Thus,
         * we put this check in place to terminate the looping early with the appropriate
         * error. Think of it as a verification step--at this stage in optimization, we
         * should have an optimal join order and conglomerate choices. If we don't, give up.
         */
        optimizer.verifyBestPlanFound();

        /* Get the cost */
        costEstimate=optimizer.getOptimizedCost();

        costEstimate.setSingleRow(optimizer.isSingleRow());
    }

    /**
     * Modify the access paths according to the decisions the optimizer
     * made.  This can include adding project/restrict nodes,
     * index-to-base-row nodes, etc.
     *
     * @param predList A list of optimizable predicates that should
     *                 be pushed to this ResultSetNode, as determined by optimizer.
     * @return The modified query tree
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode modifyAccessPaths(PredicateList predList) throws StandardException{
        // Take the received list of predicates and propagate them to the
        // predicate list for this node's optimizer.  Then, when we call
        // optimizer.modifyAccessPaths(), the optimizer will have the
        // predicates and can push them down as necessary, according
        // the join order that it has chosen.

        assert optimizer!=null: "SelectNode's optimizer not expecte to be null when modifying access paths.";

        optimizer.addScopedPredicatesToList(predList);
        return modifyAccessPaths();
    }

    /**
     * Modify the access paths according to the choices the optimizer made.
     *
     * @throws StandardException Thrown on error
     * @return A QueryTree with the necessary modifications made
     */
    @Override
    public ResultSetNode modifyAccessPaths() throws StandardException{
        int origFromListSize=fromList.size();
        ResultColumnList leftRCList;
        ResultColumnList rightRCList;
        ResultSetNode leftResultSet;
        ResultSetNode rightResultSet;

        /*
        ** Modify the access path for each Optimizable, as necessary
        **
        ** This should be the same optimizer we got above.
        */
        optimizer.modifyAccessPaths(null);

        // Load the costEstimate for the final "best" join order.
        costEstimate=optimizer.getFinalCost();

        if(SanityManager.DEBUG){
            // When we optimized this select node, we may have added pushable
            // outer predicates to the wherePredicates list for this node
            // (see the optimize() method above).  When we did so, we said
            // that all such predicates should have been removed from the
            // where list by the time optimization was completed.   So we
            // check that here, just to be safe.  NOTE: We do this _after_
            // calling optimizer.modifyAccessPaths(), because it's only in
            // that call that the scoped predicates are officially pushed
            // and thus removed from the list.
            if(wherePredicates!=null){
                Predicate pred;
                for(int i=wherePredicates.size()-1;i>=0;i--){
                    pred=(Predicate)wherePredicates.getOptPredicate(i);
                    if(pred.isScopedForPush()){
                        SanityManager.THROWASSERT("Found scoped predicate "+
                                pred.binaryRelOpColRefsToString()+
                                " in WHERE list when no scoped predicates were"+
                                " expected.");
                    }
                }
            }
        }

        selectSubquerys.modifyAccessPaths();

        if(whereSubquerys!=null && !whereSubquerys.isEmpty()){
            whereSubquerys.modifyAccessPaths();
        }

        if(havingSubquerys!=null && !havingSubquerys.isEmpty()){
            havingSubquerys.modifyAccessPaths();
        }

        /* Build a temp copy of the current FromList for sort elimination, etc. */
        preJoinFL.removeAllElements();
        preJoinFL.nondestructiveAppend(fromList);

        /* Now we build a JoinNode tree from the bottom up until there is only
         * a single entry in the fromList and that entry points to the top of
         * the JoinNode tree.
         *
         * While there is still more than 1 entry in the list, create a JoinNode
         * which points to the 1st 2 entries in the list.  This JoinNode becomes
         * the new 1st entry in the list and the 2nd entry is deleted.  The
         * old 1st and 2nd entries will get shallow copies of their
         * ResultColumnLists.  The JoinNode's ResultColumnList will be the
         * concatenation of the originals from the old 1st and 2nd entries.
         * The virtualColumnIds will be updated to reflect there new positions
         * and each ResultColumn.expression will be replaced with a new
         * VirtualColumnNode.
         */
        while(fromList.size()>1){
            /* Get left's ResultColumnList, assign shallow copy back to it
             * and create new VirtualColumnNodes for the original's
             * ResultColumn.expressions.
             */
            leftResultSet=(ResultSetNode)fromList.elementAt(0);

            leftRCList=leftResultSet.getResultColumns();
            leftResultSet.setResultColumns(leftRCList.copyListAndObjects());
            leftRCList.genVirtualColumnNodes(leftResultSet,leftResultSet.resultColumns);

            for (int i = 0; i < leftRCList.size(); ++i) {
                ResultColumn rc = leftRCList.elementAt(i);
                rc.setFromLeftChild(true);
            }

            /* Get right's ResultColumnList, assign shallow copy back to it,
             * create new VirtualColumnNodes for the original's
             * ResultColumn.expressions and increment the virtualColumnIds.
             * (Right gets appended to left, so only right's ids need updating.)
             */
            rightResultSet=(ResultSetNode)fromList.elementAt(1);

            rightRCList=rightResultSet.getResultColumns();
            rightResultSet.setResultColumns(rightRCList.copyListAndObjects());
            for (int i = 0; i < rightRCList.size(); ++i) {
                ResultColumn rc = rightRCList.elementAt(i);
                rc.setFromLeftChild(false);
            }
            rightRCList.genVirtualColumnNodes(rightResultSet,rightResultSet.resultColumns);
            rightRCList.adjustVirtualColumnIds(leftRCList.size());

//            if(leftBaseTable != null && rightBaseTable != null){
//                int size = rightBaseTable.restrictionList.size();
//
//                for(int i=0; i < size; i++){
//
//                    ColumnReference colRef = getJoinColumn((Predicate) rightBaseTable.restrictionList.getOptPredicate(i));
//
//                    if(colRef.getTableNumber() == leftBaseTable.getTableNumber()){
//                        leftBaseTable.storeRestrictionList.addPredicate(createNotNullPredicate(colRef));
//                    }
//
//                }
//            }

            /* Concatenate the 2 ResultColumnLists */
            leftRCList.nondestructiveAppend(rightRCList);

            /* Now we're finally ready to generate the JoinNode and have it
             * replace the 1st 2 entries in the FromList.
             */
            JoinNode joinNode;
            if (rightResultSet.getFromSSQ() || rightResultSet instanceof FromTable && ((FromTable)rightResultSet).getOuterJoinLevel() > 0) {
                rightRCList.setNullability(true);
                joinNode = (JoinNode)getNodeFactory().getNode(
                        C_NodeTypes.HALF_OUTER_JOIN_NODE,
                        leftResultSet,
                        rightResultSet,
                        null,                          // join clause
                        null,                                // using clause
                        Boolean.FALSE,                       // is right join
                        leftRCList,                          // RCL
                        null,                                // table props
                        getContextManager());
            } else {
                joinNode = (JoinNode) getNodeFactory().getNode(
                        C_NodeTypes.JOIN_NODE,
                        leftResultSet,
                        rightResultSet,
                        null,
                        null,
                        leftRCList,
                        null,
                        //user supplied optimizer overrides
                        fromList.properties,
                        getContextManager()
                );
            }

            ResultSetNode newPRNode = joinNode.genProjectRestrict();

            // apply post outer join conditions
            if (((FromTable)rightResultSet).getOuterJoinLevel() > 0) {
                PredicateList postJoinPredicates = ((FromTable)rightResultSet).getPostJoinPredicates();
                if (postJoinPredicates != null) {
                    for (int i=0; i<postJoinPredicates.size(); i++) {
                        Predicate pred = postJoinPredicates.elementAt(i);
                        RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
                        pred.getAndNode().accept(rcrv);
                        newPRNode.addNewPredicate(pred);
                    }
                    ((FromTable)rightResultSet).setPostJoinPredicates(null);
                }
            }

            fromList.setElementAt(newPRNode,
                    0
            );

            fromList.removeElementAt(1);
        }

        return genProjectRestrict(origFromListSize);

    }

    /**
     * Get the final CostEstimate for this SelectNode.
     *
     * @return The final CostEstimate for this SelectNode, which is
     * the final cost estimate for the best join order of
     * this SelectNode's optimizer.
     */
    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        if (optimizer == null)
            return new CostEstimateImpl();

        return optimizer.getFinalCost();
    }

    /**
     * Search to see if a query references the specifed table name.
     *
     * @param name      Table name (String) to search for.
     * @param baseTable Whether or not name is for a base table
     * @throws StandardException Thrown on error
     * @return true if found, else false
     */
    @Override
    public boolean referencesTarget(String name,boolean baseTable) throws StandardException{
        return fromList.referencesTarget(name,baseTable)
                || (selectSubquerys!=null && selectSubquerys.referencesTarget(name,baseTable))
                || (whereSubquerys!=null && whereSubquerys.referencesTarget(name,baseTable));
    }

    /**
     * Bind any untyped null nodes to the types in the given ResultColumnList.
     *
     * @param bindingRCL The ResultColumnList with the types to bind to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL) throws StandardException{
        fromList.bindUntypedNullsToResultColumns(bindingRCL);
    }

    /**
     * Get the lock mode for the target of an update statement
     * (a delete or update).  The update mode will always be row for
     * CurrentOfNodes.  It will be table if there is no where clause.
     *
     * @return The lock mode
     * @see TransactionController
     */
    @Override
    public int updateTargetLockMode(){
        /* Do row locking if there is a restriction */
        return fromList.updateTargetLockMode();
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @throws StandardException Thrown on error
     * @return true if references SESSION schema tables, else false
     */
    @Override
    public boolean referencesSessionSchema() throws StandardException{
        return fromList.referencesSessionSchema()
                || (selectSubquerys!=null && selectSubquerys.referencesSessionSchema())
                || (whereSubquerys!=null && whereSubquerys.referencesSessionSchema());

    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        if(fromList!=null){
            fromList=(FromList)fromList.accept(v, this);
        }
        if(whereClause!=null){
            whereClause=(ValueNode)whereClause.accept(v, this);
        }
        if(wherePredicates!=null){
            wherePredicates=(PredicateList)wherePredicates.accept(v, this);
        }
        if(havingClause!=null){
            havingClause=(ValueNode)havingClause.accept(v, this);
        }
    }

    /**
     * @return true if there are aggregates in the select list.
     */
    public boolean hasAggregatesInSelectList(){
        if(selectAggregates.isEmpty()){
            return false;
        }
        boolean hasAggregates=false;
        for(AggregateNode aggregateNode : selectAggregates){
            if(!aggregateNode.isWindowFunction()){
                hasAggregates=true;
            }
        }
        return hasAggregates;
    }

    /**
     * Used by SubqueryNode to avoid flattening of a subquery if a window is
     * defined on it. Note that any inline window definitions should have been
     * collected from both the selectList and orderByList at the time this
     * method is called, so the windowDefinitionList list is complete. This is true after
     * preprocess is completed.
     *
     * @return true if this select node has any windows on it
     */
    public boolean hasWindows(){ return windowDefinitionList != null; }

    /**
     * Determine whether or not the specified name is an exposed name in
     * the current query block.
     *
     * @param name       The specified name to search for as an exposed name.
     * @param schemaName Schema name, if non-null.
     * @param exactMatch Whether or not we need an exact match on specified schema and table
     *                   names or match on table id.
     * @return The FromTable, if any, with the exposed name.
     * @throws StandardException Thrown on error
     */
    @Override
    protected FromTable getFromTableByName(String name,String schemaName,boolean exactMatch) throws StandardException{
        return fromList.getFromTableByName(name,schemaName,exactMatch);
    }

    boolean hasDistinct(){ return isDistinct; }

    /**
     * Push an expression into this SELECT (and possibly down into
     * one of the tables in the FROM list).  This is useful when
     * trying to push predicates into unflattened views or
     * derived tables.
     *
     * @param predicate The predicate that we attempt to push
     * @throws StandardException Thrown on error
     */
    void pushExpressionsIntoSelect(Predicate predicate) throws StandardException{
        wherePredicates.pullExpressions(referencedTableMap.size(),predicate.getAndNode());
        Boolean disableTC = (Boolean)getLanguageConnectionContext().getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.DISABLE_TC_PUSHED_DOWN_INTO_VIEWS);
        if (disableTC == null || !disableTC) {
            if (fromList.size() > 1) {
                performTransitiveClosure();
            }
        }
        fromList.pushPredicates(wherePredicates);
    }

    /**
     * Push the order by list down from the cursor node
     * into its child result set so that the optimizer
     * has all of the information that it needs to
     * consider sort avoidance.
     *
     * @param orderByList The order by list
     */
    @Override
    void pushOrderByList(OrderByList orderByList){
        this.orderByList=orderByList;
        // remember that there was an order by list
        orderByQuery=true;
    }

    /**
     * Push down the offset and fetch first parameters to this node.
     *
     * @param offset             the OFFSET, if any
     * @param fetchFirst         the OFFSET FIRST, if any
     * @param hasJDBClimitClause true if the clauses were added by (and have the semantics of) a JDBC limit clause
     */
    @Override
    void pushOffsetFetchFirst(ValueNode offset,ValueNode fetchFirst,boolean hasJDBClimitClause){
        this.offset=offset;
        this.fetchFirst=fetchFirst;
        this.hasJDBClimitClause=hasJDBClimitClause;
    }

    /**
     * Determine if this select is updatable or not, for a cursor.
     */
    @Override
    boolean isUpdatableCursor(DataDictionary dd) throws StandardException{
        TableDescriptor targetTableDescriptor;

        if(isDistinct){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select has distinct");
            return false;
        }

        if((selectAggregates==null) || (!selectAggregates.isEmpty())){
            return false;
        }

        if(groupByList!=null || havingClause!=null){
            return false;
        }

        assert fromList!=null: "Select must have from tables";
        if(fromList.size()!=1){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select has more than one from table");
            return false;
        }

        targetTable=(FromTable)(fromList.elementAt(0));

        if(targetTable instanceof FromVTI){
            return ((FromVTI)targetTable).isUpdatableCursor();
        }

        if(!(targetTable instanceof FromBaseTable)){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select has non base table as target table");
            return false;
        }


         /* Get the TableDescriptor and verify that it is not for a
          * view or a system table.
          * NOTE: We need to use the base table name for the table.
          *         Simplest way to get it is from a FromBaseTable.  We
          *         know that targetTable is a FromBaseTable because of check
          *         just above us.
         * NOTE: We also need to use the base table's schema name; otherwise
         *        we will think it is the default schema Beetle 4417
          */
        targetTableDescriptor=getTableDescriptor(targetTable.getBaseTableName(),
                getSchemaDescriptor(((FromBaseTable)targetTable).getTableNameField().getSchemaName()));
        assert targetTableDescriptor!=null;
        if(targetTableDescriptor.getTableType()==TableDescriptor.SYSTEM_TABLE_TYPE){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select is on system table");
            return false;
        }
        if(targetTableDescriptor.getTableType()==TableDescriptor.EXTERNAL_TYPE){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select is on system table");
            return false;
        }
        if(targetTableDescriptor.getTableType()==TableDescriptor.VIEW_TYPE){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select is on view");
            return false;
        }
        if((getSelectSubquerys()!=null) && (!getSelectSubquerys().isEmpty())){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select has subquery in SELECT list");
            return false;
        }

        if((getWhereSubquerys()!=null) && (!getWhereSubquerys().isEmpty())){
            if(SanityManager.DEBUG)
                SanityManager.DEBUG("DumpUpdateCheck","cursor select has subquery in WHERE clause");
            return false;
        }

        return true;
    }

    /**
     * Assumes that isCursorUpdatable has been called, and that it
     * is only called for updatable cursors.
     */
    @Override
    FromTable getCursorTargetTable(){
        assert targetTable!=null: "must call isUpdatableCursor() first, and must be updatable";
        return targetTable;
    }

    /**
     * Return whether or not this ResultSetNode contains a subquery with a
     * reference to the specified target table.
     *
     * @param name      The table name.
     * @param baseTable Whether or not table is a base table.
     * @return boolean    Whether or not a reference to the table was found.
     * @throws StandardException Thrown on error
     */
    @Override
    boolean subqueryReferencesTarget(String name,boolean baseTable) throws StandardException{
        return (selectSubquerys!=null && selectSubquerys.referencesTarget(name,baseTable))
                || (whereSubquerys!=null && whereSubquerys.referencesTarget(name,baseTable));
    }

    /**
     * Decrement (query block) level (0-based) for
     * all of the tables in this ResultSet tree.
     * This is useful when flattening a subquery.
     *
     * @param decrement The amount to decrement by.
     */
    @Override
    void decrementLevel(int decrement){
        /* Decrement the level in the tables */
        fromList.decrementLevel(decrement);
        selectSubquerys.decrementLevel(decrement);
        whereSubquerys.decrementLevel(decrement);
        /* Decrement the level in any CRs in predicates
         * that are interesting to transitive closure.
         */
        wherePredicates.decrementLevel(fromList,decrement);
    }

    /**
     * Determine whether or not this subquery,
     * the SelectNode is in a subquery, can be flattened
     * into the outer query block based on a uniqueness condition.
     * A uniqueness condition exists when we can guarantee
     * that at most 1 row will qualify in each table in the
     * subquery.  This is true if every table in the from list is
     * (a base table and the set of columns from the table that
     * are in equality comparisons with expressions that do not
     * include a column from the same table is a superset of any unique index
     * on the table) or an ExistsBaseTable.
     *
     * @param additionalEQ Whether or not the column returned
     *                     by this select, if it is a ColumnReference,
     *                     is in an equality comparison.
     * @throws StandardException Thrown on error
     * @return Whether or not this subquery can be flattened based
     * on a uniqueness condition.
     */
    boolean uniqueSubquery(boolean additionalEQ) throws StandardException{
        ColumnReference additionalCR=null;
        ResultColumn rc=getResultColumns().elementAt(0);

        /* Figure out if we have an additional ColumnReference
         * in an equality comparison.
         */
        if(additionalEQ && rc.getExpression() instanceof ColumnReference){
            additionalCR=(ColumnReference)rc.getExpression();

            /* ColumnReference only interesting if it is
             * not correlated.
             */
            if(additionalCR.getCorrelated()){
                additionalCR=null;
            }
        }

        ResultColumnList rcl=(additionalCR==null)?null:getResultColumns();
        return fromList.returnsAtMostSingleRow(rcl, whereClause,wherePredicates );
    }

    /**
     * Return whether or not this ResultSet tree is guaranteed to return
     * at most 1 row based on heuristics.  (A RowResultSetNode and a
     * SELECT with a non-grouped aggregate will return at most 1 row.)
     *
     * @return Whether or not this ResultSet tree is guaranteed to return
     * at most 1 row based on heuristics.
     */
    @Override
    boolean returnsAtMostOneRow() throws StandardException {
        return groupByList==null && selectAggregates!=null && !selectAggregates.isEmpty() ||
                LimitOffsetVisitor.fetchNumericValue(fetchFirst) == 1;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * A no-op for SelectNode.
     */
    @Override
    void replaceOrForbidDefaults(TableDescriptor ttd,
                                 ResultColumnList tcl,
                                 boolean allowDefaults) throws StandardException{
    }

    private WindowList addInlinedWindowDefinition(WindowList wl, WindowFunctionNode wfn) throws StandardException{
        WindowDefinitionNode wdn=(WindowDefinitionNode)wfn.getWindow();

        if(wl==null){
            // This is the first window we see, so initialize list.
            wl=new WindowList();
            wl.setContextManager(getContextManager());
        }

        WindowDefinitionNode equiv=wdn.findEquivalentWindow(wl);

        // add window functions requiring identical window definitions to window a given
        // definition here, where they can share.
        if(equiv!=null){
            // If the window is equivalent an existing one, optimize
            // it away.
            wfn.setWindow(equiv);
            equiv.addWindowFunction(wfn);
        }else{
            // remember this window for posterity
            wl.addWindow(wfn.getWindow());
            wfn.getWindow().addWindowFunction(wfn);
        }

        return wl;
    }


    /*
        From where clause, this method returns a collection of all predicates that are eligible as a hash join predicate.
        That is, the operator is an equal operator, both operands are column references.
     */
    private void collectJoinPredicates(ValueNode n,List<BinaryRelationalOperatorNode> l){

        if(n instanceof BinaryRelationalOperatorNode){
            BinaryRelationalOperatorNode cNode=(BinaryRelationalOperatorNode)n;
            if(cNode.operator.compareTo("=")==0
                    && cNode.leftOperand instanceof ColumnReference
                    && cNode.rightOperand instanceof ColumnReference){

                l.add((BinaryRelationalOperatorNode)n);
            }
        }else if(n instanceof BinaryOperatorNode){
            collectJoinPredicates(((BinaryOperatorNode)n).leftOperand,l);
            collectJoinPredicates(((BinaryOperatorNode)n).rightOperand,l);
        }else if(n instanceof UnaryOperatorNode){
            collectJoinPredicates(((UnaryOperatorNode)n).operand,l);
        }
    }

    /**
     * Peform the various types of transitive closure on the where clause.
     * The 2 types are transitive closure on join clauses and on search clauses.
     * Join clauses will be processed first to maximize benefit for search clauses.
     *
     * @throws StandardException Thrown on error
     */
    private void performTransitiveClosure() throws StandardException{
        // Join clauses
        wherePredicates.joinClauseTransitiveClosure(fromList,getCompilerContext());

        // Search clauses
        wherePredicates.searchClauseTransitiveClosure(fromList.hashJoinSpecified());
    }

    /**
     * Put the expression trees in conjunctive normal form
     *
     * @param boolClause clause to normalize
     * @throws StandardException Thrown on error
     */
    public static ValueNode normExpressions(ValueNode boolClause) throws StandardException{
        /* For each expression tree:
         *    o Eliminate NOTs (eliminateNots())
         *    o Ensure that there is an AndNode on top of every
         *      top level expression. (putAndsOnTop())
         *    o Finish the job (changeToCNF())
         */
        if(boolClause!=null){
            boolClause=boolClause.eliminateNots(false);
            if(SanityManager.DEBUG){
                if(!(boolClause.verifyEliminateNots())){
                    boolClause.treePrint();
                    SanityManager.THROWASSERT("boolClause in invalid form: "+boolClause);
                }
            }
            boolClause=boolClause.putAndsOnTop();
            if(SanityManager.DEBUG){
                if(!((boolClause instanceof AndNode) && (boolClause.verifyPutAndsOnTop()))){
                    boolClause.treePrint();
                    SanityManager.THROWASSERT("boolClause in invalid form: "+boolClause);
                }
            }
            boolClause=boolClause.changeToCNF(true);
            if(SanityManager.DEBUG){
                if(!((boolClause instanceof AndNode) && (boolClause.verifyChangeToCNF()))){
                    boolClause.treePrint();
                    SanityManager.THROWASSERT("boolClause in invalid form: "+boolClause);
                }
            }
        }

        return boolClause;
    }

    /**
     * Is the result of this node an ordered result set.  An ordered result set
     * means that the results from this node will come in a known sorted order.
     * This means that the data is ordered according to the order of the elements in the RCL.
     * Today, the data is considered ordered if:
     * o The RCL is composed entirely of CRs or ConstantNodes
     * o The underlying tree is ordered on the CRs in the order in which
     * they appear in the RCL, taking equality predicates into account.
     * Future Enhancements:
     * o The prefix will not be required to be in order.  (We will need to
     * reorder the RCL and generate a PRN with an RCL in the expected order.)
     *
     * @return boolean    Whether or not this node returns an ordered result set.
     * @throws StandardException Thrown on error
     */
    private boolean isOrderedResult(ResultColumnList resultColumns,
                                    ResultSetNode newTopRSN,
                                    boolean permuteOrdering) throws StandardException{
        int rclSize=resultColumns.size();

        /* Not ordered if RCL contains anything other than a ColumnReference
         * or a ConstantNode.
         */
        int numCRs=0;
        for(int index=0;index<rclSize;index++){
            ResultColumn rc=resultColumns.elementAt(index);
            if(rc.getExpression() instanceof ColumnReference){
                numCRs++;
            }else if(!(rc.getExpression() instanceof ConstantNode)){
                return false;
            }
        }

        // Corner case, all constants
        if(numCRs==0){
            return true;
        }

        ColumnReference[] crs=new ColumnReference[numCRs];

        // Now populate the CR array and see if ordered
        int crsIndex=0;
        for(int index=0;index<rclSize;index++){
            ResultColumn rc=resultColumns.elementAt(index);
            if(rc.getExpression() instanceof ColumnReference){
                crs[crsIndex++]=(ColumnReference)rc.getExpression();
            }
        }

        return newTopRSN.isOrderedOn(crs,permuteOrdering,null);
    }

    private FromBaseTable getBaseTableNode(ResultSetNode rsn){

        FromBaseTable table=null;

        if(rsn instanceof FromBaseTable){
            table=(FromBaseTable)rsn;
        }else if(rsn instanceof ProjectRestrictNode){
            ProjectRestrictNode prn=(ProjectRestrictNode)rsn;
            table=getBaseTableNode(prn.getChildResult());
        }

        return table;
    }

    private void pullRowIdPredicates(ProjectRestrictNode prn) throws StandardException{
        ResultSetNode rsn = prn.getChildResult();
        if (rsn instanceof ProjectRestrictNode) {
            ProjectRestrictNode child = (ProjectRestrictNode) rsn;
            child.pullRowIdPredicates(prn.getRestrictionList());
        }
    }

    public GroupByList getGroupByList() {
        return groupByList;
    }

    public void setGroupByList(GroupByList groupByList) {
        this.groupByList = groupByList;
    }

    public List<AggregateNode> getSelectAggregates() {
        return selectAggregates;
    }

    public List<AggregateNode> getWhereAggregates() {
        return whereAggregates;
    }

    public List<AggregateNode> getHavingAggregates() {
        return havingAggregates;
    }

    public OrderByList getOrderByList() {
        return orderByList;
    }

    public static class SelectNodeWithSubqueryPredicate implements splice.com.google.common.base.Predicate<Visitable> {
        @Override
        public boolean apply(Visitable input) {
            return (input instanceof SelectNode) && (!((SelectNode) input).getWhereSubquerys().isEmpty() || !((SelectNode) input).getSelectSubquerys().isEmpty());
        }
    }
    public static class SelectNodeNestingLevelFunction implements splice.com.google.common.base.Function<SelectNode, Integer> {
        @Override
        public Integer apply(SelectNode input) {
            return input.getNestingLevel();
        }
    }



    public boolean isUnsatisfiable() {
        if (sat == Satisfiability.UNSAT)
            return true;

        if (sat != Satisfiability.UNKNOWN)
            return false;


        //if the query contains aggregate but no group by clause
        //even with unsatisfiable condition, one row will be returned,
        //so don't mark it as unsatisfiable
        //e.g., 1 row is returned for the following query:
        //select max(a1) from t1 where 1=0;
        if (selectAggregates != null && !selectAggregates.isEmpty() ||
                havingAggregates != null && !havingAggregates.isEmpty()) {
            if (groupByList == null || groupByList.isEmpty()) {
                sat = Satisfiability.NEITHER;
                return false;
            }
        }

        if (isNonAggregatePartUnsat()) {
            sat = Satisfiability.UNSAT;
            return true;
        }

        sat = Satisfiability.NEITHER;
        return false;
    }

    /* the non-aggregate part is unsatisfiable if:
       1. its wherePredicate contains an unsat condition (1=0) (assume CNF form); or
       2. its fromList contains a node that is unsat
     */

    public boolean isNonAggregatePartUnsat() {
        if (nonAggregatePartSat == Satisfiability.UNSAT)
            return true;

        if (nonAggregatePartSat != Satisfiability.UNKNOWN)
            return false;

        if (wherePredicates != null) {
            if (wherePredicates.isUnsatisfiable()) {
                nonAggregatePartSat = Satisfiability.UNSAT;
                return true;
            }
        }

        for (int i=0; i<fromList.size(); i++) {
            FromTable ft = (FromTable)fromList.elementAt(i);
            // do not consider flattened inner table nodes from outer joins
            if ((ft.getOuterJoinLevel() == 0) && ft.isUnsatisfiable()) {
                nonAggregatePartSat = Satisfiability.UNSAT;
                return true;
            }
        }
        nonAggregatePartSat = Satisfiability.NEITHER;
        return false;
    }

    /**
     * If the non-aggregate portion of the SelectNode is not satisfiable, perform a rewrite by
     * replacing the non-aggregate portion to a simple RowResultSetNode which represents one row of all nulls.
     * For example, the below function will rewrite the query:
     *     select a1, a2 from t1 left join t2 on a1=a2 where 1=0;
     * to
           select a1, a2 from (values (null, null)) as dt (a1,a2) where 1=0;
     */
    public Visitable unsatTreePruning() throws StandardException {
        // check if the non-aggregate part of the Select is unsatisfiable
        if (!isNonAggregatePartUnsat())
            return this;

        if (fromList.isEmpty())
            return this;
        // if the SelectNode contains only one RowResultSetNode, no need to do this optimization
        if (fromList.size() == 1) {
            FromTable ft = (FromTable)fromList.elementAt(0);
            if (ft instanceof ProjectRestrictNode) {
                if (((ProjectRestrictNode) ft).getChildResult() instanceof RowResultSetNode)
                    return this;
            }
        }

        NodeFactory nf = getNodeFactory();
        ContextManager cm = getContextManager();

        // create a RCL based on the current RCL list in the fromList
        // this will be the new RowResultSetNode's RCL
        ResultColumnList rowRCL = null;
        for (int i=0; i<fromList.size(); i++) {
            FromTable ft = (FromTable)fromList.elementAt(i);
            ResultColumnList tmpRCL = ft.getResultColumns();
            ft.setResultColumns(tmpRCL.copyListAndObjects());
            if (rowRCL == null)
                rowRCL = tmpRCL;
            else {
                rowRCL.appendResultColumns(tmpRCL, true);
            }
        }

        // replace the source of each result column with null value
        for (int i=0; i < rowRCL.size(); i++) {
            ResultColumn rc = rowRCL.elementAt(i);
            if (rc.getTypeId() == null)
                throw StandardException.newException("Type in Result Column is not specified");
            if (rc.getTypeId().getJDBCTypeId() == Types.REF) {
                ValueNode rowLocationNode = (ValueNode) getNodeFactory().getNode(
                        C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
                        getContextManager());
                rc.setExpression(rowLocationNode);

            } else {
                rc.setExpression(getNullNode(rc.getTypeServices()));
            }
        }

        // Manufacture a RowResultSetNode
        RowResultSetNode rowResultSetNode =(RowResultSetNode) nf.getNode(
                                    C_NodeTypes.ROW_RESULT_SET_NODE,
                                    rowRCL,
                                    null,
                                    cm);
        FromList tmpFromList = (FromList)nf.getNode(
                C_NodeTypes.FROM_LIST,
                nf.doJoinOrderOptimization(),
                cm);
        rowResultSetNode.bindExpressions(tmpFromList);
        rowResultSetNode.setLevel(((FromTable)fromList.elementAt(0)).getLevel());

        // reuse an existing tableNumber from the fromList
        int reusedTableNumber = referencedTableMap.getFirstSetBit();
        rowResultSetNode.setTableNumber(reusedTableNumber);
        /* Allocate a dummy referenced table map */
        int numTables = getCompilerContext().getMaximalPossibleTableCount();
        JBitSet tableMap = new JBitSet(numTables);
        tableMap.set(reusedTableNumber);
        rowResultSetNode.setReferencedTableMap(tableMap);


        ResultColumnList prRCL = rowRCL;
        rowRCL = rowRCL.copyListAndObjects();
        rowResultSetNode.setResultColumns(rowRCL);
        prRCL.genVirtualColumnNodes(rowResultSetNode, rowRCL);

        //generate UNSAT condition
        Predicate unsatPredicate = Predicate.generateUnsatPredicate(numTables, nf, cm);
        PredicateList predList = (PredicateList)nf.getNode( C_NodeTypes.PREDICATE_LIST, cm);
        predList.addPredicate(unsatPredicate);

        // Add ProjectRestrictNode ontop with unsat condition
        ProjectRestrictNode newPRN = (ProjectRestrictNode) nf.getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                rowResultSetNode,        /* Child ResultSet */
                prRCL,    /* Projection */
                null,            /* Restriction */
                predList,            /* Restriction as PredicateList */
                null,            /* Subquerys in Projection */
                null,            /* Subquerys in Restriction */
                null,          /* table properties */
                getContextManager()     );

        newPRN.setLevel(rowResultSetNode.getLevel());
        // set referenced tableMap for the PRN
        newPRN.setReferencedTableMap((JBitSet)tableMap.clone());

        fromList.removeAllElements();
        fromList.addElement(newPRN);

        //update other fields in the current SelectNode
        referencedTableMap.clearAll();
        referencedTableMap.or(tableMap);
        wherePredicates.removeAllElements();
        whereSubquerys.removeAllElements();

        //if query contains aggregation and group by
        return this;
    }

    public List<QueryTreeNode> collectReferencedColumns() throws StandardException {
        CollectingVisitor<QueryTreeNode> cnVisitor = new ColumnCollectingVisitor(
                Predicates.or(Predicates.instanceOf(ColumnReference.class),
                              Predicates.instanceOf(VirtualColumnNode.class),
                              Predicates.instanceOf(OrderedColumn.class)));
        // collect column references from different components

        if (whereClause != null)
            whereClause.accept(cnVisitor);
        if (groupByList != null)
            groupByList.accept(cnVisitor);
        if (orderByList != null)
            orderByList.accept(cnVisitor);

        if (selectAggregates != null && !selectAggregates.isEmpty()) {
            for (AggregateNode aggrNode: selectAggregates)
                aggrNode.accept(cnVisitor);
        }

        /**
         * Currently, this code will not be triggered as we don't support aggregate in where.
         * If it is not empty, it should have errored out in binding phase.
         */
        if (whereAggregates != null && !whereAggregates.isEmpty()) {
            for (AggregateNode aggrNode: whereAggregates)
                aggrNode.accept(cnVisitor);
        }

        if (havingAggregates != null && !havingAggregates.isEmpty()) {
            for (AggregateNode aggrNode: havingAggregates)
                aggrNode.accept(cnVisitor);
        }

        /**
         * we do not really need to check for having clause as all column references in having clause
         * should either appear in GroupBy or aggregate expression
         */

        /* where predicates should not have been populated before preprocess(), so no need to check here */

        /* we don't need to explicitly check for window function as it will appear in resultcolumns also */

        /** we don't need to explicitly check for whereSubquery as it is covered in whereClause,
         *  we don't need to explicitly check for havingSubquery for the same reason as havingClause mentioned above
         *  we don't need to explicit check for selectSubquery as it is covered in SelectClause
         */

        /* check for column references in select list */
        for (int i=0; i<resultColumns.size(); i++) {
            ResultColumn rc = resultColumns.elementAt(i);

            if (rc.isReferenced())
                rc.accept(cnVisitor);
        }

        return cnVisitor.getCollected();

    }

    /**
     * prune the unreferenced result columns of FromSubquery node and FromBaseTable node
     */
    public Visitable projectionListPruning(boolean considerAllRCs) throws StandardException {
        // mark all result columns as referenced
        if (considerAllRCs || isDistinct)
            resultColumns.setColumnReferences(true, true);

        // clear the referenced fields for all tables
        for (int i=0; i<fromList.size(); i++) {
            FromTable fromTable = (FromTable) fromList.elementAt(i);

            ResultColumnList rcl = fromTable.getResultColumns();
            rcl.setColumnReferences(false, true);
        }

        List<QueryTreeNode> refedcolmnList = collectReferencedColumns();

        markReferencedResultColumns(refedcolmnList);

        return this;
    }

    /**
     * Is the GROUP BY clause nonexistent or a dummy constant value?
     */
    public boolean hasNoGroupBy(){
        if (groupByList == null || groupByList.isEmpty())
            return true;
        if (groupByList.size() != 1 ||
            !(groupByList.elementAt(0) instanceof GroupByColumn))
            return false;

        GroupByColumn groupByColumn = (GroupByColumn)groupByList.elementAt(0);
        if ( !(groupByColumn.getColumnExpression() instanceof NumericConstantNode))
            return false;

        return true;
    }

    public boolean getOriginalWhereClauseHadSubqueries() {
        return this.originalWhereClauseHadSubqueries;
    }

    public boolean hasHavingClause() {
        return havingClause != null;
    }


    public JBitSet collectInnerTablesFromFlattenedOJ() {
        JBitSet collected = new JBitSet(getCompilerContext().getMaximalPossibleTableCount());
        for (int i=0; i<fromList.size(); i++) {
            FromTable fromTable = (FromTable)fromList.getOptimizable(i);
            if (fromTable.getOuterJoinLevel() > 0) {
                collected.or(fromTable.getReferencedTableMap());
            }
        }
        return collected;
    }
}
