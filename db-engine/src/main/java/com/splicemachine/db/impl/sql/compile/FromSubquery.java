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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.CollectingVisitor;
import com.splicemachine.db.impl.ast.ColumnCollectingVisitor;
import splice.com.google.common.base.Predicates;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashMap;
import java.util.List;


/**
 * A FromSubquery represents a subquery in the FROM list of a DML statement.
 *
 * The current implementation of this class is only
 * sufficient for Insert's need to push a new
 * select on top of the one the user specified,
 * to make the selected structure match that
 * of the insert target table.
 *
 */
public class FromSubquery extends FromTable
{
    ResultSetNode    subquery;
    private OrderByList orderByList;
    private ValueNode offset;
    private ValueNode fetchFirst;
    private boolean hasJDBClimitClause; // true if using JDBC limit/offset escape syntax
    static int anonymousSubqueries = 0;

    /**
     * DERBY-3270: If this subquery represents an expanded view, this holds the
     * current compilation schema at view definition time.
     */
    private SchemaDescriptor origCompilationSchema = null;

    /**
     * Initializer for a table in a FROM list.
     *
     * @param subquery        The subquery
     * @param orderByList   ORDER BY list if any, or null
     * @param offset        OFFSET if any, or null
     * @param fetchFirst    FETCH FIRST if any, or null
     * @param hasJDBClimitClause True if the offset/fetchFirst clauses come from JDBC limit/offset escape syntax
     * @param correlationName    The correlation name
     * @param derivedRCL        The derived column list
     * @param tableProperties    Properties list associated with the table
     */
    @Override
    public void init(
                    Object subquery,
                    Object orderByList,
                    Object offset,
                    Object fetchFirst,
                    Object hasJDBClimitClause,
                    Object correlationName,
                     Object derivedRCL,
                    Object tableProperties)
    {
        if (correlationName == null) {
            correlationName = getNewAnonymousCorrelationName();
        }
        super.init(correlationName, tableProperties);
        this.subquery = (ResultSetNode) subquery;
        this.orderByList = (OrderByList)orderByList;
        this.offset = (ValueNode)offset;
        this.fetchFirst = (ValueNode)fetchFirst;
        this.hasJDBClimitClause = (hasJDBClimitClause != null) && (Boolean) hasJDBClimitClause;
        resultColumns = (ResultColumnList) derivedRCL;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);

            if (subquery != null)
            {
                printLabel(depth, "subquery: ");
                subquery.treePrint(depth + 1);
            }

            if (orderByList != null)
            {
                printLabel(depth, "orderByList: ");
                orderByList.treePrint(depth + 1);
            }

            if (offset != null)
            {
                printLabel(depth, "offset: ");
                offset.treePrint(depth + 1);
            }

            if (fetchFirst != null)
            {
                printLabel(depth, "fetchFirst: ");
                fetchFirst.treePrint(depth + 1);
            }
        }
    }

    /**
     * Return the "subquery" from this node.
     *
     * @return ResultSetNode    The "subquery" from this node.
     */
    public ResultSetNode getSubquery()
    {
        return subquery;
    }

    /**
     * Determine whether or not the specified name is an exposed name in
     * the current query block.
     *
     * @param name    The specified name to search for as an exposed name.
     * @param schemaName    Schema name, if non-null.
     * @param exactMatch    Whether or not we need an exact match on specified schema and table
     *                        names or match on table id.
     *
     * @return The FromTable, if any, with the exposed name.
     *
     * @exception StandardException        Thrown on error
     */
    protected FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
        throws StandardException
    {
        if (schemaName != null && origTableName != null) {
            // View can have schema
            if (!schemaName.equals(origTableName.schemaName)) {
                return null;
            }
            // So far, so good, now go on to compare table name
        }

        if (getExposedName().equals(name)) {
            return this;
        }

        return null;
    }

    /**
     * Bind this subquery that appears in the FROM list.
     *
     * @param dataDictionary    The DataDictionary to use for binding
     * @param fromListParam        FromList to use/append to.
     *
     * @return    ResultSetNode        The bound FromSubquery.
     *
     * @exception StandardException        Thrown on error
     */

    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
                          FromList fromListParam)
                            throws StandardException
    {
        /* Assign the tableNumber */
        if (tableNumber == -1)  // allow re-bind, in which case use old number
            tableNumber = getCompilerContext().getNextTableNumber();

        subquery = subquery.bindNonVTITables(dataDictionary, fromListParam);

        return this;
    }

    /**
     * Bind this subquery that appears in the FROM list.
     *
     * @param fromListParam        FromList to use/append to.
     *
     * @return    ResultSetNode        The bound FromSubquery.
     *
     * @exception StandardException        Thrown on error
     */

    public ResultSetNode bindVTITables(FromList fromListParam)
                            throws StandardException
    {
        subquery = subquery.bindVTITables(fromListParam);

        return this;
    }

    /**
     * Check for (and reject) ? parameters directly under the ResultColumns.
     * This is done for SELECT statements.  For FromSubquery, we
     * simply pass the check through to the subquery.
     *
     * @exception StandardException        Thrown if a ? parameter found
     *                                    directly under a ResultColumn
     */

    public void rejectParameters() throws StandardException
    {
        subquery.rejectParameters();
    }

    /**
     * Bind the expressions in this FromSubquery.  This means
     * binding the sub-expressions, as well as figuring out what the return
     * type is for each expression.
     *
     * @exception StandardException        Thrown on error
     */

    public void bindExpressions(FromList fromListParam)
                    throws StandardException
    {
        FromList            emptyFromList =
                                (FromList) getNodeFactory().getNode(
                                    C_NodeTypes.FROM_LIST,
                                    getNodeFactory().doJoinOrderOptimization(),
                                    getContextManager());
        ResultColumnList    derivedRCL = resultColumns;
        ResultColumnList    subqueryRCL;
        FromList            nestedFromList;

        /* From subqueries cannot be correlated, so we pass an empty FromList
         * to subquery.bindExpressions() and .bindResultColumns()
         */
        if (orderByList != null) {
            orderByList.pullUpOrderByColumns(subquery);
        }

        nestedFromList = emptyFromList;

        CompilerContext compilerContext = getCompilerContext();

        if (origCompilationSchema != null) {
            // View expansion needs the definition time schema
            compilerContext.pushCompilationSchema(origCompilationSchema);
        }

        try {
            subquery.bindExpressions(nestedFromList);
            subquery.bindResultColumns(nestedFromList);
        } finally {
            if (origCompilationSchema != null) {
                compilerContext.popCompilationSchema();
            }
        }

        if (orderByList != null) {
            orderByList.bindOrderByColumns(subquery);
        }

        bindOffsetFetch(offset, fetchFirst);

        /* NOTE: If the size of the derived column list is less than
         * the size of the subquery's RCL and the derived column list is marked
         * for allowing a size mismatch, then we have a select * view
         * on top of a table that has had columns added to it via alter table.
         * In this case, we trim out the columns that have been added to
         * the table since the view was created.
         */
        subqueryRCL = subquery.getResultColumns();
        if (resultColumns != null && resultColumns.getCountMismatchAllowed() &&
            resultColumns.size() < subqueryRCL.size())
        {
            for (int index = subqueryRCL.size() - 1;
                 index >= resultColumns.size();
                 index--)
            {
                subqueryRCL.removeElementAt(index);
            }
        }

        /*
         * Create RCL based on subquery, adding a level of VCNs.
         */
         ResultColumnList newRcl = subqueryRCL.copyListAndObjects();
         if (getCompilerContext().isProjectionPruningEnabled())
             newRcl.genVirtualColumnNodes(subquery, subquery.getResultColumns(), false);
         else
             newRcl.genVirtualColumnNodes(subquery, subquery.getResultColumns());
         resultColumns = newRcl;

        /* Propagate the name info from the derived column list */
        if (derivedRCL != null)
        {
             resultColumns.propagateDCLInfo(derivedRCL, correlationName);
        }
    }

    /**
     * Try to find a ResultColumn in the table represented by this FromBaseTable
     * that matches the name in the given ColumnReference.
     *
     * @param columnReference    The columnReference whose name we're looking
     *                for in the given table.
     *
     * @return    A ResultColumn whose expression is the ColumnNode
     *            that matches the ColumnReference.
     *        Returns null if there is no match.
     *
     * @exception StandardException        Thrown on error
     */

    public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException
    {
        ResultColumn    resultColumn = null;
        String            columnsTableName;

        /*
        ** RESOLVE: When we add support for schemas, check to see if
        ** the column name specifies a schema, and if so, if this
        ** table is in that schema.
        */

        columnsTableName = columnReference.getTableName();

        // post 681, 1 may be no longer needed. 5 is the default case
        // now but what happens if the condition is false? Investigate.
        if (columnReference.getGeneratedToReplaceAggregate()) // 1
        {
            resultColumn = resultColumns.getResultColumn(columnReference.getColumnName());
        }
        else if (columnsTableName == null || columnsTableName.equals(correlationName)) // 5?
        {
            resultColumn = resultColumns.getAtMostOneResultColumn(columnReference, correlationName, false, true);
        }


        if (resultColumn != null)
        {
            if (resultColumn.pulledupOrderingColumn()) {
                // do not return match if the ordering column is not only in ordering list
                return null;
            }
            columnReference.setTableNumber(tableNumber);
            columnReference.setColumnNumber(resultColumn.getColumnPosition());
        }

        return resultColumn;
    }

    /**
     * Preprocess a ResultSetNode - this currently means:
     *    o  Generating a referenced table map for each ResultSetNode.
     *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
     *  o  Converting the WHERE and HAVING clauses into PredicateLists and
     *       classifying them.
     *  o  Ensuring that a ProjectRestrictNode is generated on top of every
     *     FromBaseTable and generated in place of every FromSubquery.
     *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
     *
     * @param numTables            The number of tables in the DML Statement
     * @param gbl                The group by list, if any
     * @param fromList            The from list, if any
     *
     * @return ResultSetNode at top of preprocessed tree.
     *
     * @exception StandardException        Thrown on error
     */

    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH", justification = "DB-9844")
    public ResultSetNode preprocess(int numTables,
                                    GroupByList gbl,
                                    FromList fromList)
                                throws StandardException
    {

        if(subquery != null && subquery instanceof SelectNode){
            ResultColumnList rcl = subquery.resultColumns;
            HashMap<String, ResultColumn> hs = new HashMap<>();

            for(ResultColumn rc : rcl){
                hs.put(rc.name, rc);
            }

            if(((SelectNode) subquery).groupByList != null){
                for(OrderedColumn gbc : ((SelectNode) subquery).groupByList){
                    if(gbc.getColumnExpression() instanceof ColumnReference
                            && (hs.containsKey(((ColumnReference) gbc.getColumnExpression()).columnName))){
                            ResultColumn rc = hs.get(((ColumnReference) gbc.getColumnExpression()).columnName);
                            rc.isGenerated = false;
                            /**
                             * We've determined whether a RC is referenced or not at the beginning of optimization stage
                             * through ProjectionPruningVisitor, and rely on this setting to determine if an RC need to
                             * be pruned or preserved, so do not overwrite it
                             */
                    //        rc.isReferenced = false;
                            rc.isGroupingColumn = true;
                    }
                }
            }
        }

        // Push the order by list down to the ResultSet
        if (orderByList != null)
        {
            // If we have more than 1 ORDERBY columns, we may be able to
            // remove duplicate columns, e.g., "ORDER BY 1, 1, 2".
            if (orderByList.size() > 1)
            {
                orderByList.removeDupColumns();
            }

            subquery.pushOrderByList(orderByList);
            orderByList = null;
        }

        subquery.pushOffsetFetchFirst( offset, fetchFirst, hasJDBClimitClause );

        /* We want to chop out the FromSubquery from the tree and replace it
         * with a ProjectRestrictNode.  One complication is that there may be
         * ColumnReferences above us which point to the FromSubquery's RCL.
         * What we want to return is a tree with a PRN with the
         * FromSubquery's RCL on top.  (In addition, we don't want to be
         * introducing any redundant ProjectRestrictNodes.)
         * Another complication is that we want to be able to only push
         * projections and restrictions down to this ProjectRestrict, but
         * we want to be able to push them through as well.
         * So, we:
         *        o call subquery.preprocess() which returns a tree with
         *          a SelectNode or a RowResultSetNode on top.
         *        o If the FSqry is flattenable(), then we return (so that the
         *          caller can then call flatten()), otherwise we:
         *        o generate a PRN, whose RCL is the FSqry's RCL, on top of the result.
         *        o create a referencedTableMap for the PRN which represents
         *          the FSqry's tableNumber, since ColumnReferences in the outer
         *          query block would be referring to that one.
         *          (This will allow us to push restrictions down to the PRN.)
         */

        subquery = subquery.preprocess(numTables, gbl, fromList);

        /* Return if the FSqry is flattenable()
         * NOTE: We can't flatten a FromSubquery if there is a group by list
         * because the group by list must be ColumnReferences.  For:
         *    select c1 from v1 group by c1,
         *    where v1 is select 1 from t1
         * The expression under the last redundant ResultColumn is an IntConstantNode,
         * not a ColumnReference.
         * We also do not flatten a subquery if tableProperties is non-null,
         * as the user is specifying 1 or more properties for the derived table,
         * which could potentially be lost on the flattening.
         * RESOLVE - this is too restrictive.
         */
        if ((gbl == null || gbl.isEmpty()) &&
            tableProperties == null &&
            subquery.flattenableInFromSubquery(fromList))
        {
            /* Set our table map to the subquery's table map. */
            setReferencedTableMap(subquery.getReferencedTableMap());
            return this;
        }

        return extractSubquery(numTables);
    }

    /**
     * Extract out and return the subquery, with a PRN on top.
     * (See FromSubquery.preprocess() for more details.)
     *
     * @param numTables            The number of tables in the DML Statement
     *
     * @return ResultSetNode at top of extracted tree.
     *
     * @exception StandardException        Thrown on error
     */

    public ResultSetNode extractSubquery(int numTables)
        throws StandardException
    {
        JBitSet          newJBS;
        ResultSetNode newPRN;

        newPRN = (ResultSetNode) getNodeFactory().getNode(
                                C_NodeTypes.PROJECT_RESTRICT_NODE,
                                subquery,        /* Child ResultSet */
                                resultColumns,    /* Projection */
                                null,            /* Restriction */
                                null,            /* Restriction as PredicateList */
                                null,            /* Subquerys in Projection */
                                null,            /* Subquerys in Restriction */
                                tableProperties,
                                getContextManager()     );

        /* Set up the PRN's referencedTableMap */
        newJBS = new JBitSet(numTables);
        newJBS.set(tableNumber);
        if (referencedTableMap == null) {
            referencedTableMap = subquery.referencedTableMap;
        }
        newJBS.or(referencedTableMap);

        newPRN.setReferencedTableMap(newJBS);
        ((FromTable) newPRN).setTableNumber(tableNumber);

        // carry-over the fromSSQ and dependencyMap
        if (fromSSQ) {
            ((FromTable) newPRN).setFromSSQ(true);
            ((FromTable) newPRN).setOuterJoinLevel(getOuterJoinLevel());
            ((FromTable) newPRN).setExistsTable(existsTable, isNotExists, matchRowId);
            ((FromTable) newPRN).setDependencyMap(dependencyMap);
        }

        return newPRN;
    }

    /**
     * Flatten this FSqry into the outer query block. The steps in
     * flattening are:
     *    o  Mark all ResultColumns as redundant, so that they are "skipped over"
     *       at generate().
     *    o  Append the wherePredicates to the outer list.
     *    o  Return the fromList so that the caller will merge the 2 lists
     *  RESOLVE - FSqrys with subqueries are currently not flattenable.  Some of
     *  them can be flattened, however.  We need to merge the subquery list when
     *  we relax this restriction.
     *
     * NOTE: This method returns NULL when flattening RowResultSetNodes
     * (the node for a VALUES clause).  The reason is that no reference
     * is left to the RowResultSetNode after flattening is done - the
     * expressions point directly to the ValueNodes in the RowResultSetNode's
     * ResultColumnList.
     *
     * @param rcl                The RCL from the outer query
     * @param outerPList    PredicateList to append wherePredicates to.
     * @param sql                The SubqueryList from the outer query
     * @param gbl                The group by list, if any
     * @param havingClause      The HAVING clause, if any
     * @param numTables     maximum number of tables in the query
     *
     * @return FromList        The fromList from the underlying SelectNode.
     *
     * @exception StandardException        Thrown on error
     */
    public FromList flatten(ResultColumnList rcl,
                            PredicateList outerPList,
                            SubqueryList sql,
                            GroupByList gbl,
                            ValueNode havingClause,
                            int numTables)

            throws StandardException
    {
        FromList    fromList = null;
        SelectNode    selectNode;

        resultColumns.setRedundant();

        subquery.getResultColumns().setRedundant();

        /*
        ** RESOLVE: Each type of result set should know how to remap itself.
        */
        if (subquery instanceof SelectNode)
        {
            selectNode = (SelectNode) subquery;
            fromList = selectNode.getFromList();

            // selectNode.getResultColumns().setRedundant();

            PredicateList wherePredicates = selectNode.getWherePredicates();
            if (fromSSQ) {
                // some predicates may have been pushed to each individual fromtable in FromList(currently
                // we only support one fromTable element in FromList to be able to be flattened),
                // we need to pull them out, so that they can be marked with the right OuterJoinLevel
                for (int i = 0; i < fromList.size(); i++) {
                    FromTable ft = (FromTable) (fromList.elementAt(i));
                    ft.pullOptPredicates(wherePredicates);
                }
            }
            if (!wherePredicates.isEmpty())
            {
                if (fromSSQ) {
                    // before releasing these predicates to the main SELECT, we need to mark the predicates with the
                    // right OuterJoinLevel, so that the can be consumed at the right outer join
                    for(int index=0;index<wherePredicates.size();index++) {
                        wherePredicates.elementAt(index).setOuterJoinLevel(getOuterJoinLevel());
                    }
                }
                outerPList.destructiveAppend(wherePredicates);
            }

            if (!selectNode.getWhereSubquerys().isEmpty())
            {
                sql.destructiveAppend(selectNode.getWhereSubquerys());
            }

            // if SSQ, need to transfer the SSQ properties to the tables in from list
            if (fromSSQ) {
                assert fromList.size() <= 1: "Scalar subquery with more than one tables in fromList cannot be flattened.";

                FromTable ft = (FromTable)(fromList.elementAt(0));



                ft.setFromSSQ(fromSSQ);
                ft.setOuterJoinLevel(this.getOuterJoinLevel());
                ft.setExistsTable(existsTable, isNotExists, matchRowId);
                ft.setDependencyMap(this.dependencyMap);
            }

        }
        else if ( ! (subquery instanceof RowResultSetNode))
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("subquery expected to be either a SelectNode or a RowResultSetNode, but is a " + subquery.getClass().getName());
            }
        }

        /* Remap all ColumnReferences from the outer query to this node.
         * (We replace those ColumnReferences with clones of the matching
         * expression in the SELECT's RCL.
         */
        rcl.remapColumnReferencesToExpressions();
        outerPList.remapColumnReferencesToExpressions();
        if (gbl != null)
        {
            gbl.remapColumnReferencesToExpressions();
        }

        if (havingClause != null) {
            havingClause.remapColumnReferencesToExpressions();
        }

        return fromList;
    }

    /**
     * Get the exposed name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @return    The exposed name for this table.
     */

    public String getExposedName()
    {
        return correlationName;
    }

    /**
     * Expand a "*" into a ResultColumnList with all of the
     * result columns from the subquery.
     * @exception StandardException        Thrown on error
     */
    public ResultColumnList getAllResultColumns(TableName allTableName)
            throws StandardException
    {
        ResultColumnList rcList = null;
        TableName         exposedName;
        TableName        toCompare;


        if(allTableName != null)
             toCompare = makeTableName(allTableName.getSchemaName(),correlationName);
        else
            toCompare = makeTableName(null,correlationName);
        
        if ( allTableName != null &&
             ! allTableName.equals(toCompare))
        {
            return null;
        }

        /* Cache exposed name for this table.
         * The exposed name becomes the qualifier for each column
         * in the expanded list.
         */
        exposedName = makeTableName(null, correlationName);

        rcList = (ResultColumnList) getNodeFactory().getNode(
                                        C_NodeTypes.RESULT_COLUMN_LIST,
                                        getContextManager());

        /* Build a new result column list based off of resultColumns.
         * NOTE: This method will capture any column renaming due to
         * a derived column list.
         */

        // Use visibleSize, because we don't want to propagate any order by
        // columns not selected.
        int rclSize = resultColumns.visibleSize();

        for (int index = 0; index < rclSize; index++)
        {
            ResultColumn resultColumn = resultColumns.elementAt(index);
            ValueNode         valueNode;
            String             columnName;

            if (resultColumn.isGenerated())
            {
                continue;
            }

            // Build a ResultColumn/ColumnReference pair for the column //
            columnName = resultColumn.getName();
            boolean isNameGenerated = resultColumn.isNameGenerated();

            /* If this node was generated for a GROUP BY, then tablename for the CR, if any,
             * comes from the source RC.
             */
            TableName tableName;

            tableName = exposedName;

            valueNode = (ValueNode) getNodeFactory().getNode(
                                            C_NodeTypes.COLUMN_REFERENCE,
                                            columnName,
                                            tableName,
                                            getContextManager());
            resultColumn = (ResultColumn) getNodeFactory().getNode(
                                            C_NodeTypes.RESULT_COLUMN,
                                            columnName,
                                            valueNode,
                                            getContextManager());

            resultColumn.setNameGenerated(isNameGenerated);
            // Build the ResultColumnList to return //
            rcList.addResultColumn(resultColumn);
        }
        return rcList;
    }

    /**
     * Search to see if a query references the specifed table name.
     *
     * @param name        Table name (String) to search for.
     * @param baseTable    Whether or not name is for a base table
     *
     * @return    true if found, else false
     *
     * @exception StandardException        Thrown on error
     */
    public boolean referencesTarget(String name, boolean baseTable)
        throws StandardException
    {
        return subquery.referencesTarget(name, baseTable);
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @return    true if references SESSION schema tables, else false
     *
     * @exception StandardException        Thrown on error
     */
    public boolean referencesSessionSchema()
        throws StandardException
    {
        return subquery.referencesSessionSchema();
    }

    /**
     * Bind any untyped null nodes to the types in the given ResultColumnList.
     *
     * @param bindingRCL    The ResultColumnList with the types to bind to.
     *
     * @exception StandardException        Thrown on error
     */
    public void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL)
                throws StandardException
    {
        subquery.bindUntypedNullsToResultColumns(bindingRCL);
    }

    /**
     * Decrement (query block) level (0-based) for this FromTable.
     * This is useful when flattening a subquery.
     *
     * @param decrement    The amount to decrement by.
     */
    void decrementLevel(int decrement)
    {
        super.decrementLevel(decrement);
        subquery.decrementLevel(decrement);
    }

    /**
     * Associate this subquery with the original compilation schema of a view.
     *
     * @param sd schema descriptor of the original compilation schema of the
     * view.
     */
    public void setOrigCompilationSchema(SchemaDescriptor sd) {
        origCompilationSchema = sd;
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        subquery.accept(v, this);

        if (orderByList != null) {
            orderByList.accept(v, this);
        }

        if (offset != null) {
            offset.accept(v, this);
        }

        if (fetchFirst != null) {
            fetchFirst.accept(v, this);
        }
    }

    public List<QueryTreeNode> collectReferencedColumns() throws StandardException {
        CollectingVisitor<QueryTreeNode> cnVisitor = new ColumnCollectingVisitor(
                Predicates.or(Predicates.instanceOf(ColumnReference.class),
                              Predicates.instanceOf(VirtualColumnNode.class),
                              Predicates.instanceOf(OrderedColumn.class)));
        // collect column references from different components
        if (orderByList != null)
            orderByList.accept(cnVisitor);

        for (int i=0; i<resultColumns.size(); i++) {
            ResultColumn rc = resultColumns.elementAt(i);
            if (rc.isReferenced())
                rc.accept(cnVisitor);
        }

        return cnVisitor.getCollected();
    }

    public Visitable projectionListPruning(boolean considerAllRCs) throws StandardException {
        // collect referenced columns.
        List<QueryTreeNode> refedcolmnList = collectReferencedColumns();

        ResultColumnList rcl = subquery.getResultColumns();
        // clear the referenced fields for both source tables
        rcl.setColumnReferences(false, true);

        markReferencedResultColumns(refedcolmnList);

        return this;
    }

    @SuppressFBWarnings(value = "DM_STRING_CTOR", justification = "DB-9844")
    private String getNewAnonymousCorrelationName() {
        return new String("_spliceinternal_anonym_subquery_" + anonymousSubqueries++);
    }
}
