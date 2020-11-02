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

import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.RequiredRowOrdering;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.sql.execute.ExecCursorTableReference;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;

import com.splicemachine.db.iapi.sql.Activation;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.reference.ClassName;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;

/**
 * The CurrentOf operator is used by positioned DELETE 
 * and UPDATE to get the current row and location
 * for the target cursor.  The bind() operations for 
 * positioned DELETE and UPDATE add a column to 
 * the select list under the statement for the row location 
 * accessible from this node.
 *
 * This node is placed in the from clause of the select
 * generated for the delete or update operation. It acts
 * much like a FromBaseTable, using the information about
 * the target table of the cursor to provide information.
 *
 */
public final class CurrentOfNode extends FromTable {

    private String                     cursorName;
    private ExecPreparedStatement     preStmt;
    private TableName                 exposedTableName;
    private TableName                 baseTableName;
    private CostEstimate             singleScanCostEstimate;

    @Override
    public boolean isParallelizable() {
        return true; //since it's for updates and deletes
    }

    //
    // initializers
    //
    public void init( Object correlationName, Object cursor, Object tableProperties)
    {
        super.init(correlationName, tableProperties);
        cursorName = (String) cursor;
    }

    /*
     * Optimizable interface
     */

    /**
     * @see Optimizable#estimateCost
     *
     * @exception StandardException        Thrown on error
     */
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                    ConglomerateDescriptor cd,
                                    CostEstimate outerCost,
                                    Optimizer optimizer,
                                    RowOrdering rowOrdering)
            throws StandardException
    {
        /*
        ** Get the cost of a single scan of this result set.
        **
        ** Assume for now that the cost of a CURRENT OF is zero, with one row
        ** fetched.  Is this true, and if not, does it make a difference?
        ** CURRENT OF can only occur when there is only one table in the
        ** FROM list, and when the only "predicate" is the WHERE CURRENT OF,
        ** so there's nothing to optimize in this case.
        */
        if (singleScanCostEstimate == null)
        {
            singleScanCostEstimate = optimizer.newCostEstimate();
        }

        singleScanCostEstimate.setCost(0.0d, 1.0d, 1.0d);
        getBestAccessPath().setCostEstimate(singleScanCostEstimate);
        getBestSortAvoidancePath().setCostEstimate(singleScanCostEstimate);

        return singleScanCostEstimate;
    }

    //
    // FromTable interface
    //

    /**
     * Binding this FromTable means finding the prepared statement
     * for the cursor and creating the result columns (the columns
     * updatable on that cursor).
     *
     * We expect someone else to verify that the target table
     * of the positioned update or delete is the table under this cursor.
     *
     * @param dataDictionary    The DataDictionary to use for binding
     * @param fromListParam        FromList to use/append to.
     *
     * @return    ResultSetNode        Returns this.
     *
     * @exception StandardException        Thrown on error
     */
    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
                           FromList fromListParam)
        throws StandardException {

        // verify that the cursor exists

        preStmt = getCursorStatement();

        if (preStmt == null) {
            throw StandardException.newException(SQLState.LANG_CURSOR_NOT_FOUND,
                        cursorName);
        }

        preStmt.rePrepare(getLanguageConnectionContext());

        // verify that the cursor is updatable (UPDATE is responsible
        // for checking that the right columns are updatable)
        if (preStmt.getUpdateMode() != CursorNode.UPDATE)
        {
            String printableString = (cursorName == null) ? "" : cursorName;
            throw StandardException.newException(SQLState.LANG_CURSOR_NOT_UPDATABLE, printableString);
        }

        ExecCursorTableReference refTab = preStmt.getTargetTable();
        String schemaName = refTab.getSchemaName();
        exposedTableName = makeTableName(null, refTab.getExposedName());
        baseTableName = makeTableName(schemaName,
                                      refTab.getBaseName());
        SchemaDescriptor tableSchema = null;
        tableSchema = getSchemaDescriptor(refTab.getSchemaName());

        /*
        ** This will only happen when we are binding against a publication
        ** dictionary w/o the schema we are interested in.
        */
        if (tableSchema == null)
        {
            throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, refTab.getSchemaName());
        }

        /* Create dependency on target table, in case table not named in
         * positioned update/delete.  Make sure we find the table descriptor,
         * we may fail to find it if we are binding a publication.
         */
        TableDescriptor td = getTableDescriptor(refTab.getBaseName(), tableSchema);

        if (td == null)
        {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, refTab.getBaseName());
        }


        /*
        ** Add all the result columns from the target table.
        ** For now, all updatable cursors have all columns
        ** from the target table.  In the future, we should
        ** relax this so that the cursor may do a partial
        ** read and then the current of should make sure that
        ** it can go to the base table to get all of the
        ** columns needed by the referencing positioned
        ** DML.  In the future, we'll probably need to get
        ** the result columns from preparedStatement and
        ** turn them into an RCL that we can run with.
        */
        resultColumns = (ResultColumnList) getNodeFactory().getNode(
                                            C_NodeTypes.RESULT_COLUMN_LIST,
                                            getContextManager());
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        int                     cdlSize = cdl.size();

        for (int index = 0; index < cdlSize; index++)
        {
            /* Build a ResultColumn/BaseColumnNode pair for the column */
            ColumnDescriptor colDesc = (ColumnDescriptor) cdl.elementAt(index);

            BaseColumnNode bcn = (BaseColumnNode) getNodeFactory().getNode(
                                            C_NodeTypes.BASE_COLUMN_NODE,
                                            colDesc.getColumnName(),
                                              exposedTableName,
                                            colDesc.getType(),
                                            getContextManager());
            ResultColumn rc = (ResultColumn) getNodeFactory().getNode(
                                            C_NodeTypes.RESULT_COLUMN,
                                            colDesc,
                                            bcn,
                                            getContextManager());

            /* Build the ResultColumnList to return */
            resultColumns.addResultColumn(rc);
        }

        /* Assign the tableNumber */
        if (tableNumber == -1)  // allow re-bind, in which case use old number
            tableNumber = getCompilerContext().getNextTableNumber();

        return this;
    }

    /**
     * Bind the expressions in this ResultSetNode.  This means binding the
     * sub-expressions, as well as figuring out what the return type is for
     * each expression.
     *
     * @param fromListParam        FromList to use/append to.
     */
    public void bindExpressions(FromList fromListParam)
    {
        /* No expressions to bind for a CurrentOfNode.
         * NOTE - too involved to optimize so that this method
         * doesn't get called, so just do nothing.
         */
    }

    /**
     * Try to find a ResultColumn in the table represented by this CurrentOfNode
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

    public ResultColumn getMatchingColumn(ColumnReference columnReference)
                        throws StandardException {

        ResultColumn    resultColumn = null;
        TableName        columnsTableName;

        columnsTableName = columnReference.getTableNameNode();

        if(columnsTableName != null)
            if(columnsTableName.getSchemaName() == null && correlationName == null)
                columnsTableName.bind(this.getDataDictionary());

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(preStmt!=null, "must have prepared statement");
        }

        /*
         * We use the base table name of the target table.
         * This is necessary since we will be comparing with the table in
         * the delete or update statement which doesn't have a correlation
         * name.  The select for which this column is created might have a
         * correlation name and so we won't find it if we look for exposed names
         * We shouldn't have to worry about multiple table since there should be
         * only one table. Beetle 4419
         */
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(baseTableName!=null,"no name on target table");
        }

        if(baseTableName != null)
            if(baseTableName.getSchemaName() == null && correlationName == null)
                baseTableName.bind(this.getDataDictionary());

        /*
         * If the column did not specify a name, or the specified name
         * matches the table we're looking at, see whether the column
         * is in this table, and also whether it is in the for update list.
        */
        if (
               (columnsTableName == null) ||
               (columnsTableName.getFullTableName().equals(baseTableName.getFullTableName())) ||
               ((correlationName != null) && correlationName.equals( columnsTableName.getTableName()))
           )
        {
            boolean notfound = false;

            resultColumn =
                resultColumns.getResultColumn(columnReference.getColumnName());

            if (resultColumn != null)
            {
                // If we found the ResultColumn, set the ColumnReference's
                // table number accordingly.  Note: we used to only set
                // the tableNumber for correlated references (as part of
                // changes for DERBY-171) but inspection of code (esp.
                // the comments in FromList.bindColumnReferences() and
                // the getMatchingColumn() methods on other FromTables)
                // suggests that we should always set the table number
                // if we've found the ResultColumn.  So we do that here.
                columnReference.setTableNumber( tableNumber );
                columnReference.setColumnNumber(
                   resultColumn.getColumnPosition());

                // If there is a result column, are we really updating it?
                // If so, verify that the column is updatable as well
                notfound =
                    (resultColumn.updatableByCursor() &&
                    !foundString(
                            preStmt.getUpdateColumns(),
                            columnReference.getColumnName()));
            }
            else
            {
                notfound = true;
            }

            if (notfound)
            {
                String printableString = (cursorName == null) ? "" : cursorName;
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_UPDATABLE_IN_CURSOR,
                         columnReference.getColumnName(), printableString);
            }
        }

        return resultColumn;
    }

    /**
     * Preprocess a CurrentOfNode.  For a CurrentOfNode, this simply means allocating
     * a referenced table map to avoid downstream NullPointerExceptions.
     * NOTE: There are no bits set in the referenced table map.
     *
     * @param numTables            The number of tables in the DML Statement
     * @param gbl                The group by list, if any
     * @param fromList            The from list, if any
     *
     * @return ResultSetNode at top of preprocessed tree.
     *
     * @exception StandardException        Thrown on error
     */

    public ResultSetNode preprocess(int numTables,
                                    GroupByList gbl,
                                    FromList fromList)
                                throws StandardException
    {
        /* Generate an empty referenced table map */
        referencedTableMap = new JBitSet(numTables);
        return this;
    }

    /**
     * Optimize this CurrentOfNode.  Nothing to do.
     *
     * @param dataDictionary    The DataDictionary to use for optimization
     * @param predicateList        The PredicateList to optimize.  This should
     *                be a single-table predicate with the table
     *                the same as the table in this FromTable.
     * @param outerRows            The number of outer joining rows
     *
     * @param forSpark
     * @return ResultSetNode    The top of the optimized subtree.
     *
     * @exception StandardException        Thrown on error
     */
    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicateList,
                                  double outerRows,
                                  boolean forSpark)
                        throws StandardException {
        /* Get an optimizer so we can get a cost */
        Optimizer optimizer = getOptimizer(
                                (FromList) getNodeFactory().getNode(
                                    C_NodeTypes.FROM_LIST,
                                    getNodeFactory().doJoinOrderOptimization(),
                                    this,
                                    getContextManager()),
                                predicateList,
                                dataDictionary,
                                (RequiredRowOrdering) null);
        optimizer.setForSpark(forSpark);

        /* Assume there is no cost associated with fetching the current row */
        bestCostEstimate = optimizer.newCostEstimate();
        bestCostEstimate.setCost(0.0d, outerRows, outerRows);

        return this;
    }

    /**
     * Generation on a CurrentOfNode creates a scan on the
     * cursor, CurrentOfResultSet.
     * <p>
     * This routine will generate and return a call of the form:
     * <pre><verbatim>
         ResultSetFactory.getCurrentOfResultSet(cursorName)
       </verbatim></pre>
     *
     * @param acb    The ActivationClassBuilder for the class being built
     * @param mb    The execute() method to be built
     *
     * @exception StandardException        Thrown on error
     */
    public void generate(ActivationClassBuilder acb,
                                MethodBuilder mb)
                            throws StandardException {

        if (SanityManager.DEBUG)
        SanityManager.ASSERT(!statementResultSet,
            "CurrentOfNode not expected to be statement node");

        /* Get the next ResultSet #, so that we can number this ResultSetNode, its
         * ResultColumnList and ResultSet.
         */
        assignResultSetNumber();

        mb.pushThis(); // for the putField

        // The generated java returned by this method is the expression:
        // ResultSetFactory.getCurrentOfResultSet(
        //        #cursorName(), this, resultSetNumber)

        acb.pushGetResultSetFactoryExpression(mb);

          mb.push(cursorName);
          acb.pushThisAsActivation(mb);
          mb.push(resultSetNumber);

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getCurrentOfResultSet",
                        ClassName.NoPutResultSet, 3);

        mb.cast(ClassName.CursorResultSet);

        // the current of scan generator is what we return
        /* This table is the target of an update or a delete, so we must
         * wrap the Expression up in an assignment expression before
         * returning. Delete or update use the field that is set
         * to calculate the CurrentRowLocation value.
         * NOTE - scanExpress is a ResultSet.  We will need to cast it to the
         * appropriate subclass.
         * For example, for a DELETE, instead of returning a call to the
         * ResultSetFactory, we will generate and return:
         *        this.SCANRESULTSET = (cast to appropriate ResultSet type)
         * The outer cast back to ResultSet is needed so that
         * we invoke the appropriate method in the call to the ResultSetFactory
         */

        mb.putField((String) null, acb.getRowLocationScanResultSetName(), ClassName.CursorResultSet);
        mb.cast(ClassName.NoPutResultSet);

        // add a check at activation reset time to see if the cursor has
        // changed underneath us. Doing it in the constructor allows the
        // compilation to happen
        MethodBuilder rmb = acb.startResetMethod();

        rmb.pushThis();
        rmb.push(cursorName);
        rmb.push(preStmt.getObjectName());
        rmb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "checkPositionedStatement",
                        "void", 2);

        rmb.methodReturn();
        rmb.complete();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */
    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);

            printLabel(depth, "cursor: ");
        }
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */
    public String toString() {
        if (SanityManager.DEBUG) {
            return "preparedStatement: " +
                (preStmt == null? "no prepared statement yet\n" :
                 preStmt.toString() + "\n")+
                cursorName + "\n" +
                super.toString();
        } else {
            return "";
        }
    }

    //
    // class interface
    //

    public String  getExposedName()
    {
        return exposedTableName.getFullTableName();
    }
    public TableName  getExposedTableName()
    {
        return exposedTableName;
    }

    public TableName  getBaseCursorTargetTableName()
    {
        return baseTableName;
    }

    public String getCursorName()
    {
        return cursorName;
    }

    /**
     * Return the CursorNode associated with a positioned update/delete.
     *
     * @return CursorNode    The associated CursorNode.
     *
     */
    ExecPreparedStatement getCursorStatement()
    {
        Activation activation = getLanguageConnectionContext().lookupCursorActivation(cursorName);

        if (activation == null)
            return null;

        return activation.getPreparedStatement();
    }

    /**
     * Get the lock mode for this table as the target of an update statement
     * (a delete or update).  This is implemented only for base tables and
     * CurrentOfNodes.
     *
     * @see TransactionController
     *
     * @return    The lock mode
     */
    public int updateTargetLockMode()
    {
        /* Do row locking for positioned update/delete */
        return TransactionController.MODE_RECORD;
    }
}
