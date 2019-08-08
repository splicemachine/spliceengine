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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizableList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.StringUtil;

import java.util.*;

/**
 * A FromList represents the list of tables in a FROM clause in a DML
 * statement.  It extends QueryTreeNodeVector.
 */

public class FromList extends QueryTreeNodeVector<QueryTreeNode> implements OptimizableList{
    Properties properties;
    // RESOLVE: The default should be false
    boolean fixedJoinOrder=true;
    // true by default.
    boolean useStatistics=true;

    // FromList could have a view in it's list. If the view is defined in SESSION
    // schema, then we do not want to cache the statement's plan. This boolean
    // will help keep track of such a condition.
    private boolean referencesSessionSchema;

    /* Whether or not this FromList is transparent.  A "transparent" FromList
     * is one in which all FromTables are bound based on an outer query's
     * FromList.  This means that the FromTables in the transparent list are
     * allowed to see and reference FromTables in the outer query's list.
     * Or put differently, a FromTable which sits in a transparent FromList
     * does not "see" the transparent FromList when binding; rather, it sees
     * (and can therefore reference) the FromList of an outer query.
     */
    private boolean isTransparent;

    /**
     * Window definitions used for resolving window functions not containing
     * in-line window specifications, but referring window definitions
     */
    private WindowList windows;


    /**
     * Initializer for a FromList
     */
    @Override
    public void init(Object optimizeJoinOrder){
        fixedJoinOrder=!((Boolean)optimizeJoinOrder);
        isTransparent=false;
    }

    /**
     * Initializer for a FromList
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object optimizeJoinOrder,Object fromTable) throws StandardException{
        init(optimizeJoinOrder);

        addFromTable((FromTable)fromTable);
    }

	/*
     * OptimizableList interface
	 */

    @Override
    public Optimizable getOptimizable(int index){
        return (Optimizable)elementAt(index);
    }

    @Override
    public void setOptimizable(int index,Optimizable optimizable){
        setElementAt((FromTable)optimizable,index);
    }

    @Override
    public void verifyProperties(DataDictionary dDictionary) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            ((Optimizable)elementAt(index)).verifyProperties(dDictionary);
        }
    }


    /**
     * Add a table to the FROM list.
     *
     * @param fromTable A FromTable to add to the list
     * @throws StandardException Thrown on error
     */
    public void addFromTable(FromTable fromTable) throws StandardException{
		/* Don't worry about checking TableOperatorNodes since
		 * they don't have exposed names.  This will potentially
		 * allow duplicate exposed names in some degenerate cases,
		 * but the binding of the ColumnReferences will catch those
		 * cases with a different error.  If the query does not have
		 * any ColumnReferences from the duplicate exposed name, the
		 * user is executing a really dumb query and we won't throw
		 * and exception - consider it an ANSI extension.
		 */
        TableName leftTable ;
        TableName rightTable ;
        if(!(fromTable instanceof TableOperatorNode)){
			/* Check for duplicate table name in FROM list */
            int size=size();
            for(int index=0;index<size;index++){
                leftTable=fromTable.getTableName();

                if(elementAt(index) instanceof TableOperatorNode){
                    continue;
                }else{
                    rightTable=((FromTable)elementAt(index)).getTableName();
                }
                if(leftTable.equals(rightTable)){
                    throw StandardException.newException(SQLState.LANG_FROM_LIST_DUPLICATE_TABLE_NAME,fromTable.getExposedName());
                }
            }
        }

        addElement(fromTable);
    }

    /**
     * Search to see if a query references the specifed table name.
     *
     * @param name      Table name (String) to search for.
     * @param baseTable Whether or not name is for a base table
     * @throws StandardException Thrown on error
     * @return true if found, else false
     */
    public boolean referencesTarget(String name,boolean baseTable) throws StandardException{
        FromTable fromTable;
        boolean found=false;

		/* Check for table or VTI name in FROM list */
        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);

            if(fromTable.referencesTarget(name,baseTable)){
                found=true;
                break;
            }
        }

        return found;
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @throws StandardException Thrown on error
     * @return true if references SESSION schema tables, else false
     */
    @Override
    public boolean referencesSessionSchema() throws StandardException{
        FromTable fromTable;
        boolean found=false;

        // Following if will return true if this FromList object had any VIEWs
        // from SESSION schema as elements.  This information is gathered during
        // the bindTables method. At the end of the bindTables, we loose
        // the information on VIEWs since they get replaced with their view
        // definition. Hence, we need to intercept in the middle on the bindTables
        // method and save that information in referencesSeesionSchema field.
        if(referencesSessionSchema) return true;

		/* Check for table or VTI name in FROM list */
        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);

            if(fromTable.referencesSessionSchema()){
                found=true;
                break;
            }
        }

        return found;
    }

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
    protected FromTable getFromTableByName(String name,String schemaName,boolean exactMatch) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);

            FromTable result=fromTable.getFromTableByName(name,schemaName,exactMatch);

            if(result!=null){
                return result;
            }
        }
        return null;
    }

    /**
     * Go through the list of the tables and see if the passed ResultColumn
     * is a join column for a right outer join with USING/NATURAL clause.
     *
     * @see HalfOuterJoinNode#isJoinColumnForRightOuterJoin
     */

    public void isJoinColumnForRightOuterJoin(ResultColumn rc){
        FromTable fromTable;
        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);
            fromTable.isJoinColumnForRightOuterJoin(rc);
        }
    }

    public void bindTables(DataDictionary dataDictionary, FromList fromListParam) throws StandardException{
        FromTable fromTable;

		/* Now we bind the tables - this is a 2 step process.
		 * We first bind all of the non-VTIs, then we bind the VTIs.
		 * This enables us to handle the passing of correlation
		 * columns in VTI parameters.
		 * NOTE: We set the table numbers for all of the VTIs in the
		 * first step, when we find them, in order to avoid an ordering
		 * problem with join columns in parameters.
		 */
        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);
            ResultSetNode newNode=fromTable.bindNonVTITables(dataDictionary,fromListParam);
            // If the fromTable is a view in the SESSION schema, then we need to save that information
            // in referencesSessionSchema element. The reason for this is that the view will get
            // replaced by it's view definition and we will loose the information that the statement
            // was referencing a SESSION schema object.
            if(fromTable.referencesSessionSchema())
                referencesSessionSchema=true;
            setElementAt(newNode,index);
        }
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);
            ResultSetNode newNode=fromTable.bindVTITables(fromListParam);
            if(fromTable.referencesSessionSchema())
                referencesSessionSchema=true;
            setElementAt(newNode,index);
        }
    }

    /**
     * Bind the expressions in this FromList.  This means
     * binding the sub-expressions, as well as figuring out what the return
     * type is for each expression.
     *
     * @throws StandardException Thrown on error
     */
    public void bindExpressions(FromList fromListParam) throws StandardException{
        FromTable fromTable;

        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);

			/* If this FromList is transparent then its FromTables should
			 * be bound based on the outer query's FROM list.
			 */
            fromTable.bindExpressions(isTransparent?fromListParam:this);
        }
    }

    /**
     * Bind the result columns of the ResultSetNodes in this FromList when there is no
     * base table to bind them to.  This is useful for SELECT statements,
     * where the result columns get their types from the expressions that
     * live under them.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */

    public void bindResultColumns(FromList fromListParam) throws StandardException{
        FromTable fromTable;

        int origList=fromListParam.size();
        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);
            if(fromTable.needsSpecialRCLBinding())
                fromTable.bindResultColumns(fromListParam);

            fromListParam.insertElementAt(fromTable,0);
        }

		/* Remove all references added here */
        while(fromListParam.size()>origList)
            fromListParam.removeElementAt(0);
    }

    /**
     * Expand a "*" into the appropriate ResultColumnList. If the "*"
     * is unqualified it will expand into a list of all columns in all
     * of the base tables in the from list at the current nesting level;
     * otherwise it will expand into a list of all of the columns in the
     * base table that matches the qualification.
     * <p/>
     * NOTE: Callers are responsible for ordering the FromList by nesting
     * level, with tables at the deepest (current) nesting level first.
     * We will expand the "*" into a list of all columns from all tables
     * having the same nesting level as the first FromTable in this list.
     * The check for nesting level exists because it's possible that this
     * FromList includes FromTables from an outer query, which can happen
     * if there is a "transparent" FromList above this one in the query
     * tree.  Ex:
     * <p/>
     * select j from onerow where exists
     * (select 1 from somerow
     * union select * from diffrow where onerow.j < diffrow.k)
     * <p/>
     * If "this" is the FromList for the right child of the UNION then it will
     * contain both "diffrow" and "onerow", the latter of which was passed
     * down via a transparent FromList (to allow binding of the WHERE clause).
     * In that case the "*" should only expand the result columns of "diffrow";
     * it should not expand the result columns of "onerow" because that table
     * is from an outer query.  We can achieve this selective expansion by
     * looking at nesting levels.
     *
     * @param allTableName The qualification on the "*" as a String.
     * @return ResultColumnList representing expansion
     * @throws StandardException Thrown on error
     */
    public ResultColumnList expandAll(TableName allTableName) throws StandardException{
        ResultColumnList resultColumnList=null;
        ResultColumnList tempRCList;
        FromTable fromTable;
 
		/* Expand the "*" for the table that matches, if it is qualified 
		 * (allTableName is not null) or for all tables in the list at the
		 * current nesting level if the "*" is not qualified (allTableName
		 * is null).  Current nesting level is determined by the nesting
		 * level of the first FromTable in the list.
		 */
        int targetNestingLevel=((FromTable)elementAt(0)).getLevel();
        int size=size();

		/* Make sure our assumption about nesting-based ordering
		 * has been satisified.  I.e. that the list is ordered
		 * with the most deeply nested FromTables first.
		 */
        if(SanityManager.DEBUG){
            int prevNL=targetNestingLevel;
            for(int i=1;i<size;i++){
                int currNL=((FromTable)elementAt(i)).getLevel();
                SanityManager.ASSERT((prevNL>=currNL),
                        "FROM list should have been ordered by nesting "+
                                "level (deepest level first), but it was not.");

                prevNL=currNL;
            }
        }

        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);
            if(targetNestingLevel!=fromTable.getLevel()){
				/* We only expand result columns for tables at the
				 * target nesting level.  Since the FromTables are
				 * sorted based on nesting level, we're done if we
				 * get here.
				 */
                break;
            }

			/* We let the FromTable decide if there is a match on
			 * the exposed name.  (A JoinNode will not have an
			 * exposed name, so it will need to pass the info to its
			 * left and right children.)
			 */
            tempRCList=fromTable.getAllResultColumns(allTableName);

            if(tempRCList==null){
                continue;
            }

			/* Expand the column list and append to the list that
			 * we will return.
			 */
            if(resultColumnList==null){
                resultColumnList=tempRCList;
            }else{
                resultColumnList.nondestructiveAppend(tempRCList);
            }
        }

		/* Give an error if the qualification name did not match 
		 * an exposed name 
		 */
        if(resultColumnList==null){
            throw StandardException.newException(SQLState.LANG_EXPOSED_NAME_NOT_FOUND,allTableName);
        }

        return resultColumnList;
    }

    /**
     * Bind a column reference to one of the tables in this FromList.  The column name
     * must be unique within the tables in the FromList.  An exception is thrown
     * if a column name is not unique.
     * <p/>
     * NOTE: Callers are responsible for ordering the FromList by nesting level,
     * with tables at the deepest (current) nesting level first.  We will try to
     * match against all FromTables at a given nesting level.  If no match is
     * found at a nesting level, then we proceed to the next level.  We stop
     * walking the list when the nesting level changes and we have found a match.
     * <p/>
     * NOTE: If the ColumnReference is qualified, then we will stop the search
     * at the first nesting level where there is a match on the exposed table name.
     * For example, s (a, b, c), t (d, e, f)
     * select * from s where exists (select * from t s where s.c = a)
     * will not find a match for s.c, which is the expected ANSI behavior.
     * <p/>
     * bindTables() must have already been called on this FromList before
     * calling this method.
     *
     * @param columnReference The ColumnReference describing the column to bind
     * @throws StandardException Thrown on error
     * @return ResultColumn    The matching ResultColumn
     */
    public ResultColumn bindColumnReference(ColumnReference columnReference) throws StandardException{
        boolean columnNameMatch=false;
        boolean tableNameMatch=false;
        FromTable fromTable;
        int currentLevel;
        int previousLevel=-1;
        ResultColumn matchingRC=null;
        ResultColumn resultColumn;
        String crTableName=columnReference.getTableName();

		/*
		** Find the first table with matching column name.  If there
		** is more than one table with a matching column name at the same
		** nesting level, give an error.
		*/
        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);

			/* We can stop if we've found a matching column or table name 
			 * at the previous nesting level.
			 */
            currentLevel=fromTable.getLevel();
            if(previousLevel!=currentLevel){
                if(columnNameMatch){
                    break;
                }

                if(tableNameMatch){
                    break;
                }
            }
			/* Simpler to always set previousLevel then to test and set */
            previousLevel=currentLevel;

            resultColumn=fromTable.getMatchingColumn(columnReference);

            if(resultColumn!=null){
                if(!columnNameMatch){
                    /* TableNumbers and column numbers are set in the CR in the
                     * underlying FromTable.  This ensures that they get the
                     * table number/column number from the underlying table,
                     * not the join node.  This is important for being able to
                     * push predicates down through join nodes.
                     */
                    matchingRC=resultColumn;
                    columnReference.setSource(resultColumn);
					/* Set the nesting level at which the CR appears and the nesting level
					 * of its source RC.
					 */
                    columnReference.setNestingLevel(((FromTable)elementAt(0)).getLevel());
                    columnReference.setSourceLevel(currentLevel);
                    columnNameMatch=true;

                    if(fromTable.isPrivilegeCollectionRequired())
                        getCompilerContext().addRequiredColumnPriv(resultColumn.getTableColumnDescriptor());
                }else{
                    throw StandardException.newException(SQLState.LANG_AMBIGUOUS_COLUMN_NAME,
                            columnReference.getSQLColumnName());
                }
            }

			/* Remember if we get a match on the exposed table name, so that
			 * we can stop at the beginning of the next level.
			 */
            tableNameMatch=tableNameMatch || (crTableName!=null && crTableName.equals(fromTable.getExposedName()));
        }

        return matchingRC;
    }

    /**
     * Check for (and reject) all ? parameters directly under the ResultColumns.
     * This is done for SELECT statements.
     *
     * @throws StandardException Thrown if a ? parameter found
     *                           directly under a ResultColumn
     */

    public void rejectParameters() throws StandardException{
        FromTable fromTable;

        int size=size();
        for(int index=0;index<size;index++){
            fromTable=(FromTable)elementAt(index);
            fromTable.rejectParameters();
        }
    }

    // This method reorders LOJs in the FROM clause.
    // For now, we process only a LOJ.  For example, "... from LOJ_1, LOJ2 ..."
    // will not be processed.
    public boolean LOJ_reorderable(int numTables) throws StandardException{
        if(size()>1)
            return false;

        FromTable ft=(FromTable)elementAt(0);

        return ft.LOJ_reorderable(numTables);
    }

    /**
     * Preprocess the query tree - this currently means:
     * o  Generating a referenced table map for each ResultSetNode.
     * o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
     * o  Converting the WHERE and HAVING clauses into PredicateLists and
     * classifying them.
     * o  Flatten those FromSubqueries which can be flattened.
     * o  Ensuring that a ProjectRestrictNode is generated on top of every
     * FromBaseTable and generated in place of every FromSubquery which
     * could not be flattened.
     * o  Pushing single table predicates down to the new ProjectRestrictNodes.
     *
     * @param numTables The number of tables in the DML Statement
     * @param gbl       The group by list, if any
     * @throws StandardException Thrown on error
     */
    public void preprocess(int numTables, GroupByList gbl, ValueNode predicateTree) throws StandardException{
        int size=size();

		/* Preprocess each FromTable in the list */
        for(int index=0;index<size;index++){
            FromTable ft=(FromTable)elementAt(index);

			/* Transform any outer joins to inner joins where appropriate */
            ft=ft.transformOuterJoins(predicateTree,numTables);
			/* Preprocess this FromTable */
            setElementAt(ft.preprocess(numTables,gbl,this),index);
        }
    }

    /**
     * Flatten all the FromTables that are flattenable.
     * RESOLVE - right now we just flatten FromSubqueries.  We
     * should also flatten flattenable JoinNodes here.
     *
     * @param rcl           The RCL from the outer query
     * @param predicateList The PredicateList from the outer query
     * @param sql           The SubqueryList from the outer query
     * @param gbl           The group by list, if any
     * @param havingClause  The HAVING clause, if any
     * @throws StandardException Thrown on error
     */
    public void flattenFromTables(ResultColumnList rcl,
                                  PredicateList predicateList,
                                  SubqueryList sql,
                                  GroupByList gbl,
                                  ValueNode havingClause) throws StandardException{
        boolean flattened=true;
        List<Integer> flattenedTableNumbers=new ArrayList<>();

        assert rcl!=null: "rcl is expected to be non-null";
        assert predicateList!=null: "predicateList is expected to be non-null";
        assert sql!=null: "sql is expected to be non-null";

		/* Loop until all flattenable entries are flattened.
		 * We restart the inner loop after flattening an in place
		 * to simplify the logic and so that we don't have to worry
		 * about walking a list while we are modifying it.
		 */
        while(flattened){
            flattened=false;

            for(int index=0;index<size() && !flattened;index++){
                FromTable ft=(FromTable)elementAt(index);

				/* Flatten FromSubquerys and flattenable JoinNodes */
                if((ft instanceof FromSubquery) || ft.isFlattenableJoinNode()){
                    //save the table number of the node to be flattened
                    flattenedTableNumbers.add(ft.getTableNumber());

					/* Remove the node from the list and insert its
					 * FromList here.
					 */
                    FromList flatteningFL=ft.flatten( rcl, predicateList, sql, gbl, havingClause);
                    assert flatteningFL==null || flatteningFL.size()>0: "flatteningFL expected to be null or size>0";

                    if(flatteningFL!=null){
                        setElementAt(flatteningFL.elementAt(0),index);

                        int innerSize=flatteningFL.size();
                        for(int inner=1;inner<innerSize;inner++){
                            insertElementAt(flatteningFL.elementAt(inner),index+inner);
                        }
                    }else{
						/*
						** If flatten returns null, that means it wants to
						** be removed from the FromList.
						*/
                        removeElementAt(index);
                    }
                    flattened=true;
                }
            }
        }
		
		/* fix up dependency maps for exists base tables since they might have a
		 * dependency on this join node
		 */
        if(!flattenedTableNumbers.isEmpty()){
            for(int i=0;i<size();i++){
                FromTable ft=(FromTable)elementAt(i);
                if(ft instanceof ProjectRestrictNode){
                    ResultSetNode rst=((ProjectRestrictNode)ft).getChildResult();
                    if(rst instanceof FromBaseTable){
                        ((FromBaseTable)rst).clearDependency(flattenedTableNumbers);
                    }
                }
            }
        }
    }

    /**
     * Categorize and push the predicates that are pushable.
     *
     * @param predicateList The query's PredicateList
     * @throws StandardException Thrown on error
     */
    void pushPredicates(PredicateList predicateList) throws StandardException{
        assert predicateList!=null: "predicateList is expected to be non-null";

		/* We can finally categorize each Predicate and try to push them down.
		 * NOTE: The PredicateList may be empty, but that's okay, we still
		 * call pushExpressions() for each entry in the FromList because that's
		 * where any outer join conditions will get pushed down.
		 */
        predicateList.categorize();

        int size=size();
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);
            fromTable.pushExpressions(predicateList);
        }
    }


    /**
     * Set the (query block) level (0-based) for the FromTables in this
     * FromList.
     *
     * @param level The query block level for this table.
     */
    public void setLevel(int level){
        int size=size();
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);
            fromTable.setLevel(level);
        }
    }

    /**
     * Set the Properties list for this FromList.
     *
     * @throws StandardException Thrown on error
     */
    public void setProperties(Properties props) throws StandardException{
        properties=props;

		/*
		** Validate the properties list now.  This is possible because
		** there is nothing in this properties list that relies on binding
		** or optimization to validate.
		*/
        Enumeration e=properties.keys();
        while(e.hasMoreElements()){
            String key=(String)e.nextElement();
            String value=(String)properties.get(key);

            switch(key){
                case "joinOrder":
                    if(StringUtil.SQLEqualsIgnoreCase(value,"fixed")){
                        fixedJoinOrder=true;
                    }else if(StringUtil.SQLEqualsIgnoreCase(value,"unfixed")){
                        fixedJoinOrder=false;
                    }else{
                        throw StandardException.newException(SQLState.LANG_INVALID_JOIN_ORDER_SPEC,value);
                    }
                    break;
                case "useStatistics":
                    if(StringUtil.SQLEqualsIgnoreCase(value,"true")){
                        useStatistics=true;
                    }else if(StringUtil.SQLEqualsIgnoreCase(value,"false")){
                        useStatistics=false;
                    }else{
                        throw StandardException.newException(SQLState.LANG_INVALID_STATISTICS_SPEC,value);
                    }
                    break;
                default:
                    throw StandardException.newException(SQLState.LANG_INVALID_FROM_LIST_PROPERTY,key,value);
            }
        }
    }

    @Override
    public void reOrder(int[] joinOrder){
        int posn;

        if(SanityManager.DEBUG){
            if(joinOrder.length!=size()){
                SanityManager.THROWASSERT("In reOrder(), FromList.size()=="+size()+",but joinOrder.length=="+joinOrder.length);
            }

			/*
			** Determine that the values in the list are unique and in range.
			** The easiest way to determine that they are unique is to add
			** them all up and see whether the result is what's expected
			** for that array size.
			*/
            int sum=0;
            int joLength=joinOrder.length-1;
            for(int i=0;i<joinOrder.length;i++){
                int jo=joinOrder[i];
                if(jo<0 || jo>joLength){
                    SanityManager.THROWASSERT(String.format("joinOrder[%d]==%d is out of range[0,%d]",i,jo,joLength));
                }

                sum+=jo;
            }

			/*
			** The sum of all integers from 0 through n is (n * (n - 1)) / 2.
			*/
            if(sum!=((joinOrder.length*(joinOrder.length-1))/2)){
                StringBuilder arrayVals= new StringBuilder();
                for(int aJoinOrder : joinOrder) arrayVals.append(aJoinOrder).append(" ");
                SanityManager.THROWASSERT("joinOrder array has some duplicate value: "+arrayVals);
            }
        }

		/* Form a list that's in the order we want */
        QueryTreeNode[] orderedFL=new FromTable[joinOrder.length];
        for(posn=0;posn<joinOrder.length;posn++){
			/*
			** Get the element at the i'th join order position from the
			** current list and make it the next element of orderedList.
			*/
            orderedFL[posn]=elementAt(joinOrder[posn]);
        }

		/* Now orderedList has been built, so set this list to the same order */
        for(posn=0;posn<joinOrder.length;posn++){
            setElementAt(orderedFL[posn],posn);
        }
    }

    @Override
    public boolean useStatistics(){
        return useStatistics;
    }

    @Override
    public boolean optimizeJoinOrder(){
        return !fixedJoinOrder;
    }

    @Override
    public boolean legalJoinOrder(int numTablesInQuery){
        JBitSet assignedTableMap=new JBitSet(numTablesInQuery);

        int size=size();
        for(int index=0;index<size;index++){
            FromTable ft=(FromTable)elementAt(index);
            assignedTableMap.or(ft.getReferencedTableMap());
            if(!ft.legalJoinOrder(assignedTableMap)){
                return false;
            }
        }
        return true;
    }

    @Override
    public void initAccessPaths(Optimizer optimizer){
        int size=size();
        for(int index=0;index<size;index++){
            FromTable ft=(FromTable)elementAt(index);
            ft.initAccessPaths(optimizer);
        }
    }

    /**
     * Bind any untyped null nodes to the types in the given ResultColumnList.
     *
     * @param bindingRCL The ResultColumnList with the types to bind to.
     * @throws StandardException Thrown on error
     */
    public void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);
            fromTable.bindUntypedNullsToResultColumns(bindingRCL);
        }
    }

    /**
     * Decrement (query block) level (0-based) for
     * all of the tables in this from list.
     * This is useful when flattening a subquery.
     *
     * @param decrement The amount to decrement by.
     */
    void decrementLevel(int decrement){
        int size=size();
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);
            fromTable.decrementLevel(decrement);

			/* Decrement the level of any CRs in single table
			 * predicates that are interesting to transitive
			 * closure.
			 */
            ProjectRestrictNode prn=(ProjectRestrictNode)fromTable;
            PredicateList pl=prn.getRestrictionList();
            if(pl!=null){
                pl.decrementLevel(this,decrement);
            }
        }
    }


    /**
     * This method is used for both subquery flattening and distinct
     * elimination based on a uniqueness condition.  For subquery
     * flattening we want to make sure that the query block
     * will return at most 1 row.  For distinct elimination we
     * want to make sure that the query block will not return
     * any duplicates.
     * This is true if every table in the from list is
     * (a base table and the set of columns from the table that
     * are in equality comparisons with expressions that do not include columns
     * from the same table is a superset of any unique index
     * on the table) or an EXISTS FBT.  In addition, at least 1 of the tables
     * in the list has a set of columns in equality comparisons with expressions
     * that do not include column references from the same query block
     * is a superset of a unique index
     * on that table.  (This ensures that the query block will onlyr
     * return a single row.)
     * This method is expected to be called after normalization and
     * after the from list has been preprocessed.
     * It can be called both before and after the predicates have
     * been pulled from the where clause.
     * The algorithm for this is as follows
     * <p/>
     * If any table in the query block is not a base table, give up.
     * For each table in the query
     * Ignore exists table since they can only produce one row
     * <p/>
     * create a matrix of tables and columns from the table (tableColMap)
     * (this is used to keep track of the join columns and constants
     * that can be used to figure out whether the rows from a join
     * or in a select list are distinct based on unique indexes)
     * <p/>
     * create an array of columns from the table(eqOuterCol)
     * (this is used to determine that only one row will be returned
     * from a join)
     * <p/>
     * if the current table is the table for the result columns
     * set the result columns in the eqOuterCol and tableColMap
     * (if these columns are a superset of a unique index and
     * all joining tables result in only one row, the
     * results will be distinct)
     * go through all the predicates and update tableColMap  and
     * eqOuterCol with join columns and correlation variables,
     * parameters and constants
     * since setting constants, correlation variables and parameters,
     * reduces the number of columns required for uniqueness in a
     * multi-column index, they are set for all the tables (if the
     * table is not the result table, in this case only the column of the
     * result table is set)
     * join columns are just updated for the column in the row of the
     * joining table.
     * <p/>
     * check if the marked columns in tableColMap are a superset of a unique
     * index
     * (This means that the join will only produce 1 row when joined
     * with 1 row of another table)
     * check that there is a least one table for which the columns in
     * eqOuterCol(i.e. constant values) are a superset of a unique index
     * (This quarantees that there will be only one row selected
     * from this table).
     * <p/>
     * Once all tables have been evaluated, check that all the tables can be
     * joined by unique index or will have only one row
     *
     * @param rcl             If non-null, the RCL from the query block.
     *                        If non-null for subqueries, then entry can
     *                        be considered as part of an = comparison.
     * @param whereClause     The WHERE clause to consider.
     * @param wherePredicates The predicates that have already been
     *                        pulled from the WHERE clause.
     * @throws StandardException Thrown on error
     * @return Whether or not query block will return
     * at most 1 row for a subquery, no duplicates
     * for a distinct.
     */
    boolean returnsAtMostSingleRow(ResultColumnList rcl,
                                   ValueNode whereClause,
                                   PredicateList wherePredicates) throws StandardException{
        boolean satisfiesOuter=false;
        int[] tableNumbers;
        ColumnReference additionalCR=null;

        PredicateList predicatesTemp;
        predicatesTemp=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());
        int wherePredicatesSize=wherePredicates.size();
        for(int index=0;index<wherePredicatesSize;index++)
            predicatesTemp.addPredicate(wherePredicates.elementAt(index));

		/* When considering subquery flattening, we are interested
		 * in the 1st (and only) entry in the RCL.  (The RCL will be
		 * null if result column is not of interest for subquery flattening.)
		 * We are interested in all entries in the RCL for distinct
		 * elimination.
		 */
        if(rcl!=null){
            ResultColumn rc=rcl.elementAt(0);
            if(rc.getExpression() instanceof ColumnReference){
                additionalCR=(ColumnReference)rc.getExpression();
            }
        }

		/* First see if all entries are FromBaseTables.  No point
		 * in continuing if not.
		 */
        int size=size();
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);
            if(!(fromTable instanceof ProjectRestrictNode)){
                return false;
            }

            ProjectRestrictNode prn=(ProjectRestrictNode)fromTable;

            if(!(prn.getChildResult() instanceof FromBaseTable)){
                return false;
            }
            FromBaseTable fbt=(FromBaseTable)prn.getChildResult();
            //Following for loop code is to take care of Derby-251 (DISTINCT returns
            //duplicate rows).
            //Derby-251 returned duplicate rows because we were looking at predicates
            //that belong to existsTable to determine DISTINCT elimination
            //
            //(Check method level comments to understand DISTINCT elimination rules.)
            //
            //For one specific example, consider the query below
            //select  distinct  q1."NO1" from IDEPT q1, IDEPT q2
            //where  ( q2."DISCRIM_DEPT" = 'HardwareDept')
            //and  ( q1."DISCRIM_DEPT" = 'SoftwareDept')  and  ( q1."NO1" <> ALL
            //(select  q3."NO1" from IDEPT q3 where  (q3."REPORTTO_NO" =  q2."NO1")))
            //(select  q3."NO1" from IDEPT q3 where  ( ABS(q3."REPORTTO_NO") =  q2."NO1")))
            //
            //Table IDEPT in the query above has a primary key defined on column "NO1"
            //This query gets converted to following during optimization
            //
            //select  distinct  q1."NO1" from IDEPT q1, IDEPT q2
            //where  ( q2."DISCRIM_DEPT" = 'HardwareDept')
            //and  ( q1."DISCRIM_DEPT" = 'SoftwareDept')  and  not exists (
            //(select  q3."NO1" from IDEPT q3 where
            //(  ( ABS(q3."REPORTTO_NO") =  q2."NO1")  and q3."NO1" = q1."NO1") ) )  ;
            //
            //For the optimized query above, Derby generates following predicates.
            //ABS(q3.reportto_no) = q2.no1
            //q2.discrim_dept = 'HardwareDept'
            //q1.descrim_dept = 'SoftwareDept'
            //q1.no1 = q3.no1
            //The predicate ABS(q3."NO1") = q1."NO1" should not be considered when trying
            //to determine if q1 in the outer query has equality comparisons.
            //Similarly, the predicate q3.reportto_no = q2.no1 should not be
            //considered when trying to determine if q2 in the outer query has
            //equality comparisons. To achieve this, predicates based on exists base
            //table q3 (the first and the last predicate) should be removed while
            //evaluating outer query for uniqueness.
            //
            if(fbt.getExistsTable()){
                int existsTableNumber=fbt.getTableNumber();
                int predicatesTempSize=predicatesTemp.size();
                for(int predicatesTempIndex=predicatesTempSize-1;
                    predicatesTempIndex>=0;predicatesTempIndex--){
                    AndNode topAndNode=predicatesTemp.elementAt(predicatesTempIndex).getAndNode();

                    for(ValueNode whereWalker=topAndNode;whereWalker instanceof AndNode;
                        whereWalker=((AndNode)whereWalker).getRightOperand()){
                        // See if this is a candidate =
                        AndNode and=(AndNode)whereWalker;

                        //we only need to worry about equality predicates because only those
                        //predicates are considered during DISTINCT elimination.
                        if(!and.getLeftOperand().isRelationalOperator() ||
                                !(((RelationalOperator)(and.getLeftOperand())).getOperator()==RelationalOperator.EQUALS_RELOP)){
                            continue;
                        }

                        JBitSet referencedTables=and.getLeftOperand().getTablesReferenced();
                        if(referencedTables.get(existsTableNumber)){
                            predicatesTemp.removeElementAt(predicatesTempIndex);
                            break;
                        }
                    }
                }
            }
        }

		/* Build an array of tableNumbers from this query block.
		 * We will use that array to find out if we have at least
		 * one table with a uniqueness condition based only on
		 * constants, parameters and correlation columns.
		 */
        tableNumbers=getTableNumbers();
        JBitSet[][] tableColMap=new JBitSet[size][size];
        boolean[] oneRow=new boolean[size];
        boolean oneRowResult;

		/* See if each table has a uniqueness condition */
        for(int index=0;index<size;index++){
            ProjectRestrictNode prn=(ProjectRestrictNode)elementAt(index);
            FromBaseTable fbt=(FromBaseTable)prn.getChildResult();

            // Skip over EXISTS FBT since they cannot introduce duplicates
            if(fbt.getExistsTable()){
                oneRow[index]=true;
                continue;
            }

            int numColumns=fbt.getTableDescriptor().getNumberOfColumns();
            boolean[] eqOuterCols=new boolean[numColumns+1];
            int tableNumber=fbt.getTableNumber();
            boolean resultColTable=false;
            JBitSet[] colMap = tableColMap[index];for(int i=0;i<size;i++)
                colMap[i]=new JBitSet(numColumns+1);

            if(additionalCR!=null && additionalCR.getTableNumber()==tableNumber){
                rcl.recordColumnReferences(eqOuterCols,colMap,index);
                resultColTable=true;
            }

			/* Now see if there are any equality conditions
			 * of interest in the where clause.
			 */
            if(whereClause!=null){
                whereClause.checkTopPredicatesForEqualsConditions(tableNumber,eqOuterCols,tableNumbers,colMap,resultColTable);
            }

			/* Now see if there are any equality conditions
			 * of interest in the where predicates.
			 */
            predicatesTemp.checkTopPredicatesForEqualsConditions(tableNumber,eqOuterCols,tableNumbers,colMap,resultColTable);

			/* Now see if there are any equality conditions
			 * of interest that were already pushed down to the
			 * PRN above the FBT. (Single table predicates.)
			 */
            if(prn.getRestrictionList()!=null){
                prn.getRestrictionList().checkTopPredicatesForEqualsConditions(tableNumber,eqOuterCols,tableNumbers,colMap,resultColTable);
            }

			/* We can finally check to see if the marked columns
			 * are a superset of any unique index.
			 */
            if(!fbt.supersetOfUniqueIndex(colMap)){
                return false;
            }
			
			/* Do we have at least 1 table whose equality condition
			 * is based solely on constants, parameters and correlation columns.
			 */
            oneRowResult=fbt.supersetOfUniqueIndex(eqOuterCols);
            if(oneRowResult){
                oneRow[index]=true;
                satisfiesOuter=true;
            }
        }

		/* Have we met all of the criteria */
        if(satisfiesOuter){
			/* check that all the tables are joined by unique indexes 
			 * or only produce 1 row
			 */
            boolean foundOneRow=true;
            while(foundOneRow){
                foundOneRow=false;
                for(int index=0;index<size;index++){
                    if(oneRow[index]){
                        for(int i=0;i<size;i++){
							/* unique key join - exists tables already marked as 
							 * 1 row - so don't need to look at them
							 */
                            if(!oneRow[i] && tableColMap[i][index].get(0)){
                                oneRow[i]=true;
                                foundOneRow=true;
                            }
                        }
                    }
                }
            }
			/* does any table produce more than one row */
            for(int index=0;index<size;index++){
                if(!oneRow[index]){
                    satisfiesOuter=false;
                    break;
                }
            }
        }
        return satisfiesOuter;
    }

    int[] getTableNumbers(){
        int size=size();
        int[] tableNumbers=new int[size];
        for(int index=0;index<size;index++){
            ProjectRestrictNode prn=(ProjectRestrictNode)elementAt(index);
            if(!(prn.getChildResult() instanceof FromTable)){
                continue;
            }
            FromTable ft=(FromTable)prn.getChildResult();
            tableNumbers[index]=ft.getTableNumber();
        }

        return tableNumbers;
    }

    /**
     * Mark all of the FromBaseTables in the list as EXISTS FBTs.
     * Each EBT has the same dependency list - those tables that are referenced
     * minus the tables in the from list.
     *
     * @param referencedTableMap The referenced table map.
     * @param outerFromList      FromList from outer query block
     * @param isNotExists        Whether or not for NOT EXISTS
     * @throws StandardException Thrown on error
     */
    void genExistsBaseTables(JBitSet referencedTableMap,
                             FromList outerFromList,
                             boolean isNotExists,
                             boolean matchRowId,
                             JBitSet correlatedTables) throws StandardException{
        JBitSet dependencyMap=(JBitSet)referencedTableMap.clone();

        // We currently only flatten single table from lists
        assert size()==1: "size() expected to be 1, not "+ size();

		/* Create the dependency map */
        int size=size();
        for(int index=0;index<size;index++){
            ResultSetNode ft=((ProjectRestrictNode)elementAt(index)).getChildResult();
            if(ft instanceof FromTable){
                dependencyMap.clear(((FromTable)ft).getTableNumber());
            }
        }

		/* Degenerate case - If flattening a non-correlated EXISTS subquery
		 * then we need to make the table that is getting flattened dependendent on
		 * all of the tables in the outer query block.  Gross but true.  Otherwise
		 * that table can get chosen as an outer table and introduce duplicates.
		 * The reason that duplicates can be introduced is that we do special processing
		 * in the join to make sure only one qualified row from the right side is
		 * returned.  If the exists table is on the left, we can return all the
		 * qualified rows. 
		 */
        if(dependencyMap.getFirstSetBit()==-1){
            int outerSize=outerFromList.size();
            for(int outer=0;outer<outerSize;outer++) {
                FromTable ft = (FromTable) outerFromList.elementAt(outer);
                // SSQ need to be processed after all the joins (including the join with where subquery) ar done,
                // so we should not include SSQs in the where subquery's dependencyMap
                // only set dependency to outer tables that the subquery is correlated to
                if (!ft.fromSSQ && correlatedTables.intersects(ft.getReferencedTableMap()))
                    dependencyMap.or(ft.getReferencedTableMap());
            }
        }

		/* Do the marking */
        for(int index=0;index<size;index++){
            FromTable fromTable=(FromTable)elementAt(index);
            if(fromTable instanceof ProjectRestrictNode){
                ProjectRestrictNode prn=(ProjectRestrictNode)fromTable;
                if(prn.getChildResult() instanceof FromBaseTable){
                    FromBaseTable fbt=(FromBaseTable)prn.getChildResult();
                    fbt.setExistsTable(true,isNotExists, matchRowId);
                    fbt.setDependencyMap((JBitSet)dependencyMap.clone());
                }
            }
        }
    }

    /**
     * determine whether this table is NOT EXISTS.
     * <p/>
     * This routine searches for the indicated table number in the fromlist
     * and returns TRUE if the table is present in the from list and is
     * marked NOT EXISTS, false otherwise.
     * <p/>
     * A table may be present in the from list for NOT EXISTS if it is used
     * as a correlated NOT EXISTS subquery. In such a situation, when the
     * subquery is flattened, it is important that we remember that this is
     * a NOT EXISTS subquery, because the join semantics are different
     * (we're looking for rows that do NOT match, rather than rows
     * that do). And since the join semantics are different, we cannot
     * include this table into a transitive closure of equijoins
     * (See DERBY-3033 for a situation where this occurs).
     *
     * @param tableNumber which table to check
     * @return true if this table is in the from list as NOT EXISTS
     */
    boolean tableNumberIsNotExists(int tableNumber) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            ProjectRestrictNode prn=(ProjectRestrictNode)elementAt(index);
            if(!(prn.getChildResult() instanceof FromTable)){
                continue;
            }
            FromTable ft=(FromTable)prn.getChildResult();
            if(ft.getTableNumber()==tableNumber)
                return ft.isNotExists();
        }
        return false;
    }

    /**
     * Get the lock mode for the target of an update statement
     * (a delete or update).  The update mode will always be row for
     * CurrentOfNodes.  It will be table if there is no where clause.
     *
     * @return The lock mode
     */
    public int updateTargetLockMode(){
        assert size()==1: "size() expected to be 1";
        return ((ResultSetNode)elementAt(0)).updateTargetLockMode();
    }

    /**
     * Return whether or not the user specified a hash join for any of the
     * tables in this list.
     *
     * @return Whether or not the user specified a hash join for any of the
     * tables in this list.
     */
    boolean hashJoinSpecified(){
        int size=size();
        for(int index=0;index<size;index++){
            FromTable ft=(FromTable)elementAt(index);
            String joinStrategy=ft.getUserSpecifiedJoinStrategy();

            if(joinStrategy!=null &&
                    (StringUtil.SQLToUpperCase(joinStrategy).equals("SORTMERGE") ||
                            StringUtil.SQLToUpperCase(joinStrategy).equals("MERGE") ||
                            StringUtil.SQLToUpperCase(joinStrategy).equals("BROADCAST"))) {
                return true;
            }
        }

        return false;
    }

    /**
     * Indicate that this FromList is "transparent", which means that
     * its FromTables should be bound to tables from an outer query.
     * Generally this is not allowed, but there are exceptions.  See
     * SetOperatorNode.setResultToBooleanTrueNode() for more.
     */
    void markAsTransparent(){
        isTransparent=true;
    }

    /**
     * Set windows field to the supplied value.
     *
     * @param windows list of window definitions associated with a SELECT.
     */
    public void setWindows(WindowList windows){
        this.windows=windows;
    }


    /**
     * @return list of window definitions associated with a SELECT.
     */
    public WindowList getWindows(){
        return windows;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setDependencyMapForSSQ(int numTables) {
        // iterate through the list to collect the tablenumber
        JBitSet dependencyMap = new JBitSet(numTables);
        for(int i=0; i<size(); i++) {
            FromTable fromTable = (FromTable) elementAt(i);
            if (!fromTable.getFromSSQ())
                dependencyMap.or(fromTable.getReferencedTableMap());
        }

        // set dependencyMap
        for (int i=0; i<size(); i++) {
            FromTable fromTable = (FromTable) elementAt(i);
            if (fromTable.getFromSSQ())
                fromTable.setDependencyMap((JBitSet)dependencyMap.clone());
        }
    }
}
