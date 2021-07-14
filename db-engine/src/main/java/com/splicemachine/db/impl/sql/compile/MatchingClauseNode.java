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

import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.lang.reflect.Modifier;
import java.util.*;

public class MatchingClauseNode extends QueryTreeNode {
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  CURRENT_OF_NODE_NAME = "$MERGE_CURRENT";

    /** clauseType for WHEN NOT MATCHED ... THEN INSERT */
    public  static  final   int WHEN_NOT_MATCHED_THEN_INSERT = 0;
    /** clauseType for WHEN MATCHED ... THEN UPDATE */
    public  static  final   int WHEN_MATCHED_THEN_UPDATE = 1;
    /** clauseType for WHEN MATCHED ... THEN DELETE */
    public  static  final   int WHEN_MATCHED_THEN_DELETE = 2;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // filled in by the constructor
    private ValueNode           _matchingRefinement;
    private ResultColumnList    _updateColumns;
    private ResultColumnList    _insertColumns;
    private ResultColumnList    _insertValues;

    //
    // filled in at bind() time
    //

    /** the INSERT/UPDATE/DELETE statement of this WHEN [ NOT ] MATCHED clause */
    private DMLModStatementNode _dml;

    /** the columns in the temporary conglomerate which drives the INSERT/UPDATE/DELETE */
    private ResultColumnList        _thenColumns;
    private int[]                           _deleteColumnOffsets;

    // Filled in at generate() time
    private int                             _clauseNumber;
    private String                          _actionMethodName;
    private String                          _resultSetFieldName;
    private String                          _rowMakingMethodName;
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS/FACTORY METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Constructor called by factory methods.
     */
    private MatchingClauseNode
    (
            ValueNode  matchingRefinement,
            ResultColumnList   updateColumns,
            ResultColumnList   insertColumns,
            ResultColumnList   insertValues,
            ContextManager     cm
    )
    {
        super( cm );

        _matchingRefinement = matchingRefinement;
        _updateColumns = updateColumns;
        _insertColumns = insertColumns;
        _insertValues = insertValues;
    }

    /** Make a WHEN MATCHED ... THEN UPDATE clause */
    public  static  MatchingClauseNode   makeUpdateClause
    (
            ValueNode  matchingRefinement,
            ResultColumnList   updateColumns,
            ContextManager     cm
    ) throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, updateColumns, null, null, cm );
    }

    /** Make a WHEN MATCHED ... THEN DELETE clause */
    public  static  MatchingClauseNode   makeDeleteClause
    (
            ValueNode  matchingRefinement,
            ContextManager     cm
    ) throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, null, null, null, cm );
    }

    /** Make a WHEN NOT MATCHED ... THEN INSERT clause */
    public  static  MatchingClauseNode   makeInsertClause
    (
            ValueNode  matchingRefinement,
            ResultColumnList   insertColumns,
            ResultColumnList   insertValues,
            ContextManager     cm
    ) throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, null, insertColumns, insertValues, cm );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if this is a WHEN MATCHED ... UPDATE clause */
    public  boolean isUpdateClause()    { return (_updateColumns != null); }

    /** Return true if this is a WHEN NOT MATCHED ... INSERT clause */
    public  boolean isInsertClause()    { return (_insertValues != null); }

    /** Return true if this is a WHEN MATCHED ... DELETE clause */
    public  boolean isDeleteClause()    { return !( isUpdateClause() || isInsertClause() ); }

    String getType() {
        if(isUpdateClause()) return "UPDATE clause";
        else if(isInsertClause()) return "INSERT clause";
        else return "DELETE clause";
    }

    /** Return the bound DML statement--returns null if called before binding */
    public  DMLModStatementNode getDML()    { return _dml; }


    /**
     * Return the list of columns which form the rows of the ResultSet which drive
     * the INSERT/UPDATE/DELETE actions.
     */
    ResultColumnList    getBufferedColumns() { return _thenColumns; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Bind this WHEN [ NOT ] MATCHED clause against the parent JoinNode */
    void    bind
    (
            DataDictionary dd,
            MergeNode mergeNode,
            FromList fullFromList,
            FromBaseTable targetTable
    )
            throws StandardException
    {
        _thenColumns = new ResultColumnList( getContextManager() );

        if ( isDeleteClause() ) { bindDelete( targetTable ); }
        if ( isUpdateClause() ) { bindUpdate( fullFromList, targetTable ); }
        if ( isInsertClause() ) { bindInsert( mergeNode, fullFromList, targetTable ); }
    }

    /** Bind the optional refinement condition in the MATCHED clause */
    void    bindRefinement( MergeNode mergeNode, FromList fullFromList ) throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            mergeNode.bindExpression( _matchingRefinement, fullFromList );
        }
    }

    /** Re-bind various clauses and lists once we have ResultSet numbers for the driving left join */
    void    bindResultSetNumbers( MergeNode mergeNode, FromList fullFromList ) throws StandardException
    {
        bindRefinement( mergeNode, fullFromList );
    }

    /** Collect the columns mentioned by expressions in this MATCHED clause */
    void    getColumnsInExpressions
    (
            MergeNode  mergeNode,
            HashMap<String,ColumnReference> drivingColumnMap
    )
            throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            mergeNode.getColumnsInExpression( drivingColumnMap, _matchingRefinement );
        }

        if ( isUpdateClause() )
        {
            TableName   targetTableName = mergeNode.getTargetTable().getTableName();

            //
            // Get all columns mentioned on both sides of SET operators in WHEN MATCHED ... THEN UPDATE clauses.
            // We need the left side because UPDATE needs before and after images of columns.
            // We need the right side because there may be columns in the expressions there.
            //
            for ( ResultColumn rc : _updateColumns )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression() );

                ColumnReference leftCR = new ColumnReference( rc.exposedName, targetTableName, getContextManager() );
                mergeNode.addColumn( drivingColumnMap, leftCR );
            }
        }
        else if ( isInsertClause() )
        {
            // get all columns mentioned in the VALUES subclauses of WHEN NOT MATCHED ... THEN INSERT clauses
            for ( ResultColumn rc : _insertValues )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression() );
            }
        }
    }

    ////////////////////// UPDATE ///////////////////////////////

    /** Bind a WHEN MATCHED ... THEN UPDATE clause */
    private void    bindUpdate
    (
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        bindSetClauses( fullFromList, targetTable );

        SelectNode selectNode = new SelectNode(
                                    _updateColumns, // selectList
                                    fullFromList, // fromList
                                    null, getContextManager()
                                );
        _dml = new UpdateNode( targetTable.getTableName(), selectNode, false, this, getContextManager() );

        _dml.bindStatement();

        //
        // Split the update row into its before and after images.
        //
        ResultColumnList    beforeColumns = new ResultColumnList( getContextManager() );
        ResultColumnList    afterColumns = new ResultColumnList( getContextManager() );
        ResultColumnList    fullUpdateRow = getBoundSelectUnderUpdate().resultColumns;

        // the full row is the before image, the after image, and a row location
        int     rowSize = fullUpdateRow.size() / 2;

        for ( int i = 0; i < rowSize; i++ )
        {
            ResultColumn    origBeforeRC = fullUpdateRow.elementAt( i );
            ResultColumn    origAfterRC = fullUpdateRow.elementAt( i + rowSize );
            ResultColumn    beforeRC = origBeforeRC.cloneMe();
            ResultColumn    afterRC = origAfterRC.cloneMe();

            beforeColumns.addResultColumn( beforeRC );
            afterColumns.addResultColumn( afterRC );
        }

        buildThenColumnsForUpdate( fullFromList, targetTable, fullUpdateRow, beforeColumns, afterColumns );
    }

    /**
     * <p>
     * Get the bound SELECT node under the dummy UPDATE node.
     * This may not be the source result set of the UPDATE node. That is because a ProjectRestrictResultSet
     * may have been inserted on top of it by DEFAULT handling. This method
     * exists to make the UPDATE actions of MERGE statement behave like ordinary
     * UPDATE statements in this situation. The behavior is actually wrong. See
     * DERBY-6414. Depending on how that bug is addressed, we may be able
     * to remove this method eventually.
     * </p>
     */
    private ResultSetNode    getBoundSelectUnderUpdate()
            throws StandardException
    {
        ResultSetNode   candidate = _dml.resultSet;

        while ( candidate != null )
        {
            if ( candidate instanceof SelectNode ) { return candidate; }
            else if ( candidate instanceof SingleChildResultSetNode )
            {
                candidate = ((SingleChildResultSetNode) candidate).getChildResult();
            }
            else    { break; }
        }

        // don't understand what's going on
        throw StandardException.newException( SQLState.NOT_IMPLEMENTED );
    }

    /** Bind the SET clauses of an UPDATE action */
    private void    bindSetClauses
    (
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        // needed to make the UpdateNode bind
        _updateColumns.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _updateColumns, true );

        bindExpressions( _updateColumns, fullFromList );
    }

    /**
     * <p>
     * Construct the row in the temporary table which drives an UPDATE action.
     * Unlike a DELETE, whose temporary row is just a list of copied columns, the
     * temporary row for UPDATE may contain complex expressions which must
     * be code-generated later on.
     * </p>
     */
    private void    buildThenColumnsForUpdate
    (
            FromList fullFromList,
            FromTable targetTable,
            ResultColumnList   fullRow,
            ResultColumnList beforeRow,
            ResultColumnList afterValues
    )
            throws StandardException
    {
        TableDescriptor td = targetTable.getTableDescriptor();
        HashSet<String> changedColumns = getChangedColumnNames();
        HashSet<String> changedGeneratedColumns = getChangedGeneratedColumnNames( td, changedColumns );

        _thenColumns = fullRow.copyListAndObjects();

        //
        // Here we set up for the evaluation of expressions in the temporary table
        // which drives the INSERT action. If we were actually generating the dummy SELECT
        // for the DML action, the work would normally be done there. But we don't generate
        // that SELECT. So we do the following here:
        //
        // 1) If a column has a value specified in the WHEN [ NOT ] MATCHED clause, then we use it.
        //     There is some special handling to make the DEFAULT value work for identity columns.
        //
        // 2) Otherwise, if the column has a default, then we plug it in.
        //
        for ( int i = 0; i < _thenColumns.size(); i++ )
        {
            ResultColumn    origRC = _thenColumns.elementAt( i );

            // skip the final RowLocation column of an UPDATE
            if(isRowLocation( origRC ))
                continue;

            boolean isAfterColumn = (i >= beforeRow.size());
            ValueNode   origExpr = origRC.getExpression();

            String           columnName = origRC.getName();
            ColumnDescriptor cd = td.getColumnDescriptor( columnName );
            boolean          changed = false;

            //
            // This handles the case that a GENERATED BY DEFAULT identity column is being
            // set to the keyword DEFAULT. This causes the UPDATE action of a MERGE statement
            // to have the same wrong behavior as a regular UPDATE statement. See derby-6414.
            //
            if ( cd.isAutoincrement() && (origRC.getExpression() instanceof NumericConstantNode) )
            {
                DataValueDescriptor numericValue = ((NumericConstantNode) origRC.getExpression()).getValue();

                if ( numericValue == null )
                {
                    ResultColumn    newRC = makeAutoGenRC( targetTable, origRC, i+1 );
                    newRC.setVirtualColumnId( origRC.getVirtualColumnId() );
                    _thenColumns.setElementAt( newRC, i  );

                    continue;
                }
            }

            //
            // VirtualColumnNodes are skipped at code-generation time. This can result in
            // NPEs when evaluating generation expressions. Replace VirtualColumnNodes with
            // UntypedNullConstantNodes, except for identity columns, which require special
            // handling below.
            //
            if ( !origRC.isAutoincrement() && (origRC.getExpression() instanceof VirtualColumnNode) )
            {
                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
            }

            //
            // Generated columns need special handling. The value needs to be recalculated
            // under the following circumstances:
            //
            // 1) It's the after image of the column
            //
            // 2) AND the statement causes the value to change.
            //
            // Otherwise, the value should be set to whatever is in the row coming out
            // of the driving left join.
            //
            if ( cd.hasGenerationClause() )
            {
                if ( isAfterColumn && changedGeneratedColumns.contains( columnName ) )
                {
                    // Set the expression to something that won't choke ResultColumnList.generateEvaluatedRow().
                    // The value will be a Java null at execution time, which will cause the value
                    // to be re-generated.
                    origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                }
                else
                {
                    ColumnReference cr = new ColumnReference
                            ( columnName, targetTable.getTableName(), getContextManager() );
                    origRC.setExpression( cr );

                    // remove the column descriptor in order to turn off hasGenerationClause()
                    origRC.columnDescriptor = null;
                }

                continue;
            }

            if ( isAfterColumn )
            {
                for ( int ic = 0; ic < beforeRow.size(); ic++ )
                {
                    ResultColumn    icRC = beforeRow.elementAt( ic );

                    if ( columnName.equals( icRC.getName() ) )
                    {
                        ResultColumn    newRC = null;

                        // replace DEFAULT for a generated or identity column
                        ResultColumn    valueRC = afterValues.elementAt( ic );

                        if ( valueRC.wasDefaultColumn() || (valueRC.getExpression() instanceof UntypedNullConstantNode ) )
                        {
                            if ( !cd.isAutoincrement() )
                            {
                                //
                                // Eliminate column references under identity columns. They
                                // will mess up the code generation.
                                //
                                ValueNode   expr = origRC.getExpression();
                                if ( expr instanceof ColumnReference )
                                {
                                    origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                                }
                                continue;
                            }

                            newRC = makeAutoGenRC( targetTable, origRC, i+1 );
                        }
                        else
                        {
                            newRC = valueRC.cloneMe();
                            newRC.setType( origRC.getTypeServices() );
                        }

                        newRC.setVirtualColumnId( origRC.getVirtualColumnId() );
                        _thenColumns.setElementAt( newRC, i  );
                        changed = true;
                        break;
                    }
                }
            }

            // plug in defaults if we haven't done so already
            if ( !changed )
            {
                DefaultInfoImpl     defaultInfo = (DefaultInfoImpl) cd.getDefaultInfo();

                if ( (defaultInfo != null) && !defaultInfo.isGeneratedColumn() && !cd.isAutoincrement() )
                {
                    _thenColumns.setDefault( origRC, cd, defaultInfo );
                    changed = true;
                }
            }

            // set the result column name correctly for buildThenColumnSignature()
            ResultColumn    finalRC = _thenColumns.elementAt( i );
            finalRC.setName( cd.getColumnName() );

            //
            // Turn off the autogenerated bit for identity columns so that
            // ResultColumnList.generateEvaluatedRow() doesn't try to compile
            // code to generate values for the before images in UPDATE rows.
            // This logic will probably need to be revisited as part of fixing derby-6414.
            //
            finalRC.resetAutoincrementGenerated();
        }   // end loop through _thenColumns
    }

    /** Get the names of the columns explicitly changed by SET clauses */
    private HashSet<String> getChangedColumnNames()
    {
        HashSet<String> result = new HashSet<String>();
        for( ResultColumn col : _updateColumns ) {
            result.add( col.getName() );
        }
        return result;
    }

    /**
     * <p>
     * Get the names of the generated columns which are changed
     * by the UPDATE statement. These are the generated columns which
     * match one of the following conditions:
     * </p>
     *
     * <ul>
     * <li>Are explicitly mentioned on the left side of a SET clause.</li>
     * <li>Are built from other columns which are explicitly mentioned on the left side of a SET clause.</li>
     * </ul>
     */
    private HashSet<String> getChangedGeneratedColumnNames
    (
            TableDescriptor    targetTableDescriptor,
            HashSet<String>    changedColumnNames  // columns which are explicitly mentioned on the left side of a SET clause
    )
            throws StandardException
    {
        HashSet<String> result = new HashSet<String>();

        for ( ColumnDescriptor cd : targetTableDescriptor.getColumnDescriptorList() )
        {
            if ( !cd.hasGenerationClause() ) { continue; }

            if ( changedColumnNames.contains( cd.getColumnName() ) )
            {
                result.add( cd.getColumnName() );
                continue;
            }

            String[] referencedColumns = cd.getDefaultInfo().getReferencedColumnNames();
            for ( String referencedColumnName : referencedColumns )
            {
                if ( changedColumnNames.contains( referencedColumnName ) )
                {
                    result.add( referencedColumnName );
                    break;
                }
            }
        }

        return result;
    }

    ////////////////////// DELETE ///////////////////////////////

    /** Bind a WHEN MATCHED ... THEN DELETE clause */
    private void bindDelete(FromBaseTable targetTable)
            throws StandardException
    {
        CurrentOfNode currentOfNode = CurrentOfNode.makeForMerge(CURRENT_OF_NODE_NAME, targetTable, getContextManager());
        FromList fromList = new FromList( getContextManager() );
        fromList.addFromTable( currentOfNode );

        SelectNode selectNode = new SelectNode( null, // selectList
                                                fromList, // fromList
                                                null, getContextManager() );
        Properties tableProperties = null; // todo: do we have to set this?
        _dml = new DeleteNode( targetTable.getTableName(), selectNode, false,
                tableProperties, this, getContextManager() );


        _dml.bindStatement();
        buildThenColumnsForDelete();
    }

    /**
     * <p>
     * Construct the signature of the temporary table which drives the
     * INSERT/UPDATE/DELETE action.
     * </p>
     */
    private void    buildThenColumnsForDelete()
            throws StandardException
    {
        ResultColumnList    dmlSignature = _dml.resultSet.resultColumns;
        for ( int i = 0; i < dmlSignature.size(); i++ )
        {
            ResultColumn    origRC = dmlSignature.elementAt( i );
            ResultColumn    newRC;
            ValueNode       expression = origRC.getExpression();

            if ( expression instanceof ColumnReference )
            {
                ColumnReference cr = (ColumnReference) ((ColumnReference) expression).getClone();
                newRC = new ResultColumn( cr, cr, getContextManager() );
            }
            else
            {
                newRC = origRC.cloneMe();
            }
            _thenColumns.addResultColumn( newRC );
        }
    }

    /**
     * <p>
     * Calculate the 1-based offsets which define the rows which will be buffered up
     * for a DELETE action at run-time. The rows are constructed
     * from the columns in the SELECT list of the driving left joins. This method
     * calculates an array of offsets into the SELECT list. The columns at those
     * offsets will form the row which is buffered up for the DELETE
     * action.
     * </p>
     */
    void    bindDeleteThenColumns( ResultColumnList selectList )
            throws StandardException
    {
        int     bufferedCount = _thenColumns.size();
        _deleteColumnOffsets = new int[ bufferedCount ];

        for ( int bidx = 0; bidx < bufferedCount; bidx++ )
        {
            ResultColumn    bufferedRC = _thenColumns.elementAt( bidx );
            ValueNode       bufferedExpression = bufferedRC.getExpression();

            _deleteColumnOffsets[ bidx ] = getSelectListOffset( selectList, bufferedExpression );
        }
    }

    ////////////////////// INSERT ///////////////////////////////

    /** Bind a WHEN NOT MATCHED ... THEN INSERT clause */
    private void    bindInsert
    (
            MergeNode  mergeNode,
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        bindInsertValues( fullFromList, targetTable );

        // following is not supported:
        //      NOT MATCHED THEN INSERT (i, j, k) VALUES (SOURCE.i, SOURCE.j, SOURCE.i+1)
        // see DB-12286
        for(ResultColumn iv : _insertValues) {
            if(iv.getExpression() instanceof BinaryOperatorNode)
                throw new RuntimeException("INSERT with expressions not supported");
        }

        // the VALUES clause may not mention columns in the target table
        FromList    targetTableFromList = new FromList( getNodeFactory().doJoinOrderOptimization(), getContextManager() );
        targetTableFromList.addElement( fullFromList.elementAt( 0 ) );
        bindExpressions( _insertValues, targetTableFromList );
        if ( _matchingRefinement != null )
        {
            mergeNode.bindExpression( _matchingRefinement, targetTableFromList );
        }

        SelectNode selectNode = new SelectNode(
                                            _insertValues, // selectList
                                            fullFromList, // fromList
                                            null, getContextManager()
                                    );
        _dml = new InsertNode
                (
                        targetTable.getTableName(),
                        _insertColumns,
                        selectNode,
                        this,      // in NOT MATCHED clause
                        null,      // targetProperties
                        null,      // order by cols
                        null,      // offset
                        null,      // fetch first
                        false,     // has JDBC limit clause
                        getContextManager()
                );

        _dml.bindStatement();
        buildThenColumnsForInsert( fullFromList, targetTable, _dml.resultSet.resultColumns, _insertColumns, _insertValues );
    }

    /**  Bind the values in the INSERT list */
    private void    bindInsertValues
    (
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        if ( _insertColumns.size() != _insertValues.size() )
        {
            throw StandardException.newException( SQLState.LANG_DB2_INVALID_COLS_SPECIFIED2,
                    _insertColumns.size(), _insertValues.size());
        }

        TableDescriptor td = targetTable.getTableDescriptor();

        // forbid illegal values for identity columns
        for ( int i = 0; i <_insertValues.size(); i++ )
        {
            ResultColumn    rc = _insertValues.elementAt( i );
            String          columnName = _insertColumns.elementAt( i ).exposedName;
            ValueNode       expr = rc.getExpression();
            ColumnDescriptor cd = td.getColumnDescriptor( columnName );

            // if the column isn't in the table, this will be sorted out when we bind
            // the InsertNode
            if ( cd == null ) { continue; }

            // DEFAULT is the only value allowed for a GENERATED ALWAYS AS IDENTITY column
            if ( cd.isAutoincAlways() && !(expr instanceof DefaultNode) )
            {
                throw StandardException.newException( SQLState.LANG_AI_CANNOT_MODIFY_AI, columnName );
            }

            // NULL is illegal as the value for any identity column
            if ( cd.isAutoincrement() && (expr instanceof UntypedNullConstantNode) )
            {
                throw StandardException.newException( SQLState.LANG_NULL_INTO_NON_NULL, columnName );
            }
        }

        // needed to make the SelectNode bind
        _insertValues.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _insertColumns, true );
        bindExpressions( _insertValues, fullFromList );
    }

    /**
     * <p>
     * Construct the row in the temporary table which drives an INSERT action.
     * Unlike a DELETE, whose temporary row is just a list of copied columns, the
     * temporary row for INSERT may contain complex expressions which must
     * be code-generated later on.
     * </p>
     */
    private void    buildThenColumnsForInsert
    (
            FromList fullFromList,
            FromTable targetTable,
            ResultColumnList   fullRow,
            ResultColumnList insertColumns,
            ResultColumnList insertValues
    )
            throws StandardException
    {
        TableDescriptor td = targetTable.getTableDescriptor();

        _thenColumns = fullRow.copyListAndObjects();

        //
        // Here we set up for the evaluation of expressions in the temporary table
        // which drives the INSERT action. If we were actually generating the dummy SELECT
        // for the DML action, the work would normally be done there. But we don't generate
        // that SELECT. So we do the following here:
        //
        // 1) If a column has a value specified in the WHEN [ NOT ] MATCHED clause, then we use it.
        //     There is some special handling to make the DEFAULT value work for identity columns.
        //
        // 2) Otherwise, if the column has a default, then we plug it in.
        //
        for ( int i = 0; i < _thenColumns.size(); i++ )
        {
            ResultColumn    origRC = _thenColumns.elementAt( i );

            String              columnName = origRC.getName();
            ColumnDescriptor    cd = td.getColumnDescriptor( columnName );
            boolean         changed = false;

            //
            // VirtualColumnNodes are skipped at code-generation time. This can result in
            // NPEs when evaluating generation expressions. Replace VirtualColumnNodes with
            // UntypedNullConstantNodes, except for identity columns, which require special
            // handling below.
            //
            if ( !origRC.isAutoincrement() && (origRC.getExpression() instanceof VirtualColumnNode) )
            {
                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
            }

            if ( cd.hasGenerationClause() )
            {
                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                continue;
            }

            for ( int ic = 0; ic < insertColumns.size(); ic++ )
            {
                ResultColumn    icRC = insertColumns.elementAt( ic );

                if ( columnName.equals( icRC.getName() ) )
                {
                    ResultColumn    newRC = null;

                    // replace DEFAULT for a generated or identity column
                    ResultColumn    valueRC = insertValues.elementAt( ic );

                    if ( valueRC.wasDefaultColumn() || (valueRC.getExpression() instanceof UntypedNullConstantNode ) )
                    {
                        if ( !cd.isAutoincrement() )
                        {
                            //
                            // Eliminate column references under identity columns. They
                            // will mess up the code generation.
                            //
                            ValueNode   expr = origRC.getExpression();
                            if ( expr instanceof ColumnReference )
                            {
                                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                            }
                            continue;
                        }

                        newRC = makeAutoGenRC( targetTable, origRC, i+1 );
                    }
                    else
                    {
                        newRC = valueRC.cloneMe();
                        newRC.setType( origRC.getTypeServices() );
                    }

                    newRC.setVirtualColumnId( origRC.getVirtualColumnId() );
                    _thenColumns.setElementAt( newRC, i  );
                    changed = true;
                    break;
                }
            }

            // plug in defaults if we haven't done so already
            if ( !changed )
            {
                DefaultInfoImpl     defaultInfo = (DefaultInfoImpl) cd.getDefaultInfo();

                if ( (defaultInfo != null) && !defaultInfo.isGeneratedColumn() && !cd.isAutoincrement() )
                {
                    _thenColumns.setDefault( origRC, cd, defaultInfo );
                    changed = true;
                }
            }

            // set the result column name correctly for buildThenColumnSignature()
            ResultColumn    finalRC = _thenColumns.elementAt( i );
            finalRC.setName( cd.getColumnName() );

        }   // end loop through _thenColumns

    }

    /**
     * <p>
     * Make a ResultColumn for an identity column which is being set to the DEFAULT
     * value. This special ResultColumn will make it through code generation so that it
     * will be calculated when the INSERT/UPDATE action is run.
     * </p>
     */
    private ResultColumn    makeAutoGenRC
    (
            FromTable targetTable,
            ResultColumn   origRC,
            int    virtualColumnID
    )
            throws StandardException
    {
        String              columnName = origRC.getName();
        ColumnReference autoGenCR = new ColumnReference( columnName, targetTable.getTableName(), getContextManager() );
        ResultColumn    autoGenRC = new ResultColumn( autoGenCR, null, getContextManager() );
        VirtualColumnNode autoGenVCN = new VirtualColumnNode( targetTable, autoGenRC, virtualColumnID, getContextManager() );
        ResultColumn    newRC = new ResultColumn( autoGenCR, autoGenVCN, getContextManager() );

        // set the type so that buildThenColumnSignature() will function correctly
        newRC.setType( origRC.getTypeServices() );

        return newRC;
    }
    ////////////////////// bind() MINIONS ///////////////////////////////

    /** Boilerplate for binding a list of ResultColumns against a FromList */
    private void bindExpressions( ResultColumnList rcl, FromList fromList )
            throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();

        try {
            cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );

            rcl.bindExpressions
                    (
                            fromList,
                            new SubqueryList( getContextManager() ),
                            new ArrayList<AggregateNode>()
                    );
        }
        finally
        {
            // Restore previous compiler state
            cc.setReliability( previousReliability );
        }
    }

    /**
     * <p>
     * Bind the row which will go into the temporary table at run-time.
     * </p>
     */
    void    bindThenColumns( ResultColumnList selectList )
            throws StandardException
    {
        if ( isDeleteClause() ) { bindDeleteThenColumns( selectList ); }
    }

    /**
     * <p>
     * Find a column reference in the SELECT list of the driving left join
     * and return its 1-based offset into that list.  Returns -1 if the column
     * can't be found.
     * </p>
     */
    private int getSelectListOffset( ResultColumnList selectList, ValueNode bufferedExpression )
            throws StandardException
    {
        int                 selectCount = selectList.size();

        if ( bufferedExpression instanceof ColumnReference )
        {
            ColumnReference bufferedCR = (ColumnReference) bufferedExpression;
            String              tableName = bufferedCR.getTableName();
            String              columnName = bufferedCR.getColumnName();

            // loop through the SELECT list to find this column reference
            for ( int sidx = 0; sidx < selectCount; sidx++ )
            {
                ResultColumn    selectRC = selectList.elementAt( sidx );
                ValueNode       selectExpression = selectRC.getExpression();
                ColumnReference selectCR = selectExpression instanceof ColumnReference ?
                        (ColumnReference) selectExpression : null;

                if ( selectCR != null )
                {
                    if (
                            tableName.equals( selectCR.getTableName() ) &&
                                    columnName.equals( selectCR.getColumnName() )
                    )
                    {
                        return sidx + 1;
                    }
                }
            }
        }
        else if ( bufferedExpression instanceof CurrentRowLocationNode )
        {
            //
            // There is only one RowLocation in the SELECT list, the row location for the
            // tuple from the target table. The RowLocation is always the last column in
            // the SELECT list.
            //
            return selectCount;
        }

        return -1;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // optimize() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Optimize the INSERT/UPDATE/DELETE action.
     * </p>
     */
    void    optimize()  throws StandardException
    {
        _dml.optimizeStatement();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // generate() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ConstantAction makeConstantAction(ActivationClassBuilder acb )
            throws StandardException
    {
        // generate the clause-specific refinement
        String  refinementName = null;
        if ( _matchingRefinement != null )
        {
            MethodBuilder userExprFun = acb.newUserExprFun();

            _matchingRefinement.generateExpression( acb, userExprFun );
            userExprFun.methodReturn();

            // we are done modifying userExprFun, complete it.
            userExprFun.complete();

            refinementName = userExprFun.getName();
        }

        return	getGenericConstantActionFactory().getMatchingClauseConstantAction
                (
                        getClauseType(),
                        refinementName,
                        buildThenColumnSignature(),
                        _rowMakingMethodName,
                        _deleteColumnOffsets,
                        _resultSetFieldName,
                        _actionMethodName,
                        _dml.makeConstantAction()
                );
    }
    private int getClauseType()
    {
        if ( isUpdateClause() ) { return MatchingClauseNode.WHEN_MATCHED_THEN_UPDATE; }
        else if ( isInsertClause() ) { return MatchingClauseNode.WHEN_NOT_MATCHED_THEN_INSERT; }
        else { return MatchingClauseNode.WHEN_MATCHED_THEN_DELETE; }
    }

    /**
     * <p>
     * Build the signature of the row which will go into the temporary table.
     * </p>
     */
    private ResultDescription buildThenColumnSignature()
            throws StandardException
    {
        ResultColumnDescriptor[]  cells = _thenColumns.makeResultDescriptors();

        return getLanguageConnectionContext().getLanguageFactory().getResultDescription( cells, "MERGE" );
    }

    /**
     * <p>
     * Generate a method to invoke the INSERT/UPDATE/DELETE action. This method
     * will be called at runtime by MatchingClauseConstantAction.executeConstantAction().
     * </p>
     */
    void    generate
    (
            ActivationClassBuilder acb,
            ResultColumnList selectList,
            ResultSetNode  generatedScan,
            HalfOuterJoinNode  hojn,
            int    clauseNumber
    )
            throws StandardException
    {
        _clauseNumber = clauseNumber;

        if ( isInsertClause() || isUpdateClause() ) {
            generateInsertUpdateRow( acb, selectList, generatedScan, hojn );
        }

        _actionMethodName = "mergeActionMethod_" + _clauseNumber;

        MethodBuilder mb = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC,
                ClassName.ResultSet, _actionMethodName);
        mb.addThrownException(ClassName.StandardException);

        remapConstraints();

        // now generate the action into this method
        _dml.generate( acb, mb );

        mb.methodReturn();
        mb.complete();
    }

    /**
     * <p>
     * Re-map ColumnReferences in constraints to point into the row from the
     * temporary table. This is where the row will be stored when constraints
     * are being evaluated.
     * </p>
     */
    private void    remapConstraints()
            throws StandardException
    {
        if( isDeleteClause()) { return; }
        else
        {
            CollectNodesVisitor getCRs = new CollectNodesVisitor(ColumnReference.class);

            ValueNode   checkConstraints = isInsertClause() ?
                    ((InsertNode) _dml).checkConstraints :
                    ((UpdateNode) _dml).checkConstraints;

            if ( checkConstraints != null )
            {
                checkConstraints.accept(getCRs);
                List<ColumnReference> colRefs = getCRs.getList();
                for ( ColumnReference cr : colRefs )
                {
                    cr.getSource().setResultSetNumber( NoPutResultSet.TEMPORARY_RESULT_SET_NUMBER );
                }
            }
        }
    }

    /**
     * <p>
     * Adds a field to the generated class to hold the ResultSet of buffered rows
     * which drive the INSERT/UPDATE/DELETE action. Generates code to push
     * the contents of that field onto the stack.
     * </p>
     */
    void    generateResultSetField( ActivationClassBuilder acb, MethodBuilder mb )
            throws StandardException
    {
        _resultSetFieldName = "mergeResultSetField_" + _clauseNumber;

        // make the field public so we can stuff it at execution time
        LocalField  resultSetField = acb.newFieldDeclaration( Modifier.PUBLIC, ClassName.NoPutResultSet, _resultSetFieldName );

        //
        // At runtime, MatchingClauseConstantAction.executeConstantAction()
        // will stuff the resultSetField with the temporary table which collects
        // the rows relevant to this action. We want to push the value of resultSetField
        // onto the stack, where it will be the ResultSet argument to the constructor
        // of the actual INSERT/UPDATE/DELETE action.
        //
        mb.getField( resultSetField );
    }

    /**
     * <p>
     * Generate a method to build a row for the temporary table for INSERT/UPDATE actions.
     * The method stuffs each column in the row with the result of the corresponding
     * expression built out of columns in the current row of the driving left join.
     * The method returns the stuffed row.
     * </p>
     */
    void    generateInsertUpdateRow
    (
            ActivationClassBuilder acb,
            ResultColumnList selectList,
            ResultSetNode  generatedScan,
            HalfOuterJoinNode  hojn
    )
            throws StandardException
    {
        // point expressions in the temporary row at the columns in the
        // result column list of the driving left join.
        adjustThenColumns( selectList, generatedScan, hojn );

        _rowMakingMethodName = "mergeRowMakingMethod_" + _clauseNumber;

        MethodBuilder mb = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC,
                ClassName.ExecRow, _rowMakingMethodName);
        mb.addThrownException(ClassName.StandardException);

        _thenColumns.generateEvaluatedRow( acb, mb, false, true );
    }

    /**
     * <p>
     * Point the column references in the temporary row at corresponding
     * columns returned by the driving left join.
     * </p>
     */
    void    adjustThenColumns
    (
            ResultColumnList selectList,
            ResultSetNode  generatedScan,
            HalfOuterJoinNode  hojn
    )
            throws StandardException
    {
        ResultColumnList    leftJoinResult = generatedScan.resultColumns;
        CollectNodesVisitor getCRs = new CollectNodesVisitor( ColumnReference.class );
        _thenColumns.accept( getCRs );

        for ( Object obj : getCRs.getList() )
        {
            ColumnReference cr = (ColumnReference) obj;
            ResultColumn    leftJoinRC = leftJoinResult.elementAt( getSelectListOffset( selectList, cr ) - 1 );
            cr.setSource( leftJoinRC );
        }

        //
        // For an UPDATE action, the final column in the temporary row is the
        // RowLocation. Point it at the last column in the row returned by the left join.
        //
        int          lastRCSlot = _thenColumns.size() - 1;
        ResultColumn lastRC = _thenColumns.elementAt( lastRCSlot );

        if ( isRowLocation( lastRC ) )
        {
            ResultColumn    lastLeftJoinRC = leftJoinResult.elementAt( leftJoinResult.size() - 1 );
            String          columnName = lastLeftJoinRC.exposedName;
            ColumnReference rowLocationCR = new ColumnReference
                    ( columnName, hojn.getTableName(), getContextManager() );

            rowLocationCR.setSource( lastLeftJoinRC );

            ResultColumn    rowLocationRC = new ResultColumn( columnName, rowLocationCR, getContextManager() );

            _thenColumns.removeElementAt( lastRCSlot );
            _thenColumns.addResultColumn( rowLocationRC );
        }
    }

    /** Return true if the ResultColumn represents a RowLocation */
    private boolean isRowLocation( ResultColumn rc ) throws StandardException
    {
        if ( rc.getExpression() instanceof CurrentRowLocationNode ) { return true; }

        DataTypeDescriptor dtd = rc.getTypeServices();
        if ( (dtd != null) && (dtd.getTypeId().isRefTypeId()) ) { return true; }

        return false;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Visitable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    @Override
    public void acceptChildren(Visitor v)
            throws StandardException
    {
        super.acceptChildren( v );

        if ( _matchingRefinement != null ) { _matchingRefinement.accept( v ); }
        if ( _updateColumns != null ) { _updateColumns.accept( v ); }
        if ( _insertColumns != null ) { _insertColumns.accept( v ); }
        if ( _insertValues != null ) { _insertValues.accept( v ); }

        if ( _dml != null ) { _dml.accept( v ); }
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return	This object as a String
     */
    @Override
    public String toString()
    {
        if ( isUpdateClause() ) { return "UPDATE"; }
        else if ( isInsertClause() ) { return "INSERT"; }
        else { return "DELETE"; }
    }

}
