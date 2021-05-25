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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Node;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

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
    private int[]                           _thenColumnOffsets;

    // Filled in at generate() time
    private int                             _clauseNumber;
    private String                          _actionMethodName;
    private String                          _resultSetFieldName;
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
    )
    {
        return new MatchingClauseNode( matchingRefinement, updateColumns, null, null, cm );
    }

    /** Make a WHEN MATCHED ... THEN DELETE clause */
    public  static  MatchingClauseNode   makeDeleteClause
    (
            ValueNode  matchingRefinement,
            ContextManager     cm
    )
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
    )
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
        bindExpressions( mergeNode, fullFromList, targetTable );

        if ( isDeleteClause() ) { bindDelete( dd, fullFromList, targetTable ); }
        if ( isUpdateClause() ) { bindUpdate( dd, fullFromList, targetTable ); }
        if ( isInsertClause() ) { bindInsert( dd, mergeNode, fullFromList, targetTable ); }

        bindExpressions( _thenColumns, fullFromList );
    }

    /** Bind the optional refinement condition in the MATCHED clause */
    void    bindRefinement( MergeNode mergeNode, FromList fullFromList ) throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            mergeNode.bindExpression( _matchingRefinement, fullFromList );
        }
    }

    /** Bind the expressions in this MATCHED clause */
    private void    bindExpressions
    (
            MergeNode mergeNode,
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        _thenColumns = new ResultColumnList( getContextManager() );

        if ( isUpdateClause() )
        {
            // needed to make the UpdateNode bind
            _updateColumns.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _updateColumns, true );

            bindExpressions( _updateColumns, fullFromList );
        }
        else if ( isInsertClause() )
        {
            // needed to make the SelectNode bind
            _insertValues.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _insertColumns, true );
            bindExpressions( _insertValues, fullFromList );
        }
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
            // get all columns mentioned on the right side of SET operators in WHEN MATCHED ... THEN UPDATE clauses
            for ( ResultColumn rc : _updateColumns )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression() );
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

    /** Bind a WHEN MATCHED ... THEN UPDATE clause */
    private void    bindUpdate
    (
            DataDictionary dd,
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        SelectNode  selectNode = new SelectNode
                (
                        _updateColumns,
                        fullFromList,
                        null,      // where clause
                        null,      // group by list
                        null,      // having clause
                        null,      // window list
                        null,      // optimizer plan override
                        getContextManager()
                );
        _dml = new UpdateNode( targetTable.getTableName(), selectNode, false, this, getContextManager() );

        _dml.bindStatement();
    }

    /** Bind a WHEN MATCHED ... THEN DELETE clause */
    private void    bindDelete
    (
            DataDictionary dd,
            FromList fullFromList,
            FromBaseTable targetTable
    )
            throws StandardException
    {
        CurrentOfNode   currentOfNode = CurrentOfNode.makeForMerge
                ( CURRENT_OF_NODE_NAME, targetTable, getContextManager() );
        FromList        fromList = new FromList( getContextManager() );
        fromList.addFromTable( currentOfNode );
        SelectNode      selectNode = new SelectNode
                (
                        null, /* selectList */
                        null, /* aggregateVector */
                        fromList, /* fromList */
                        null, /* whereClause */
                        null, /* groupByList */
                        null, /* havingClause */
                        null, /* windowDefinitionList */
                        getContextManager()
                );
        Properties tableProperties = null; // todo
        _dml = new DeleteNode( targetTable.getTableName(), selectNode, false,
                tableProperties, this, getContextManager() );

        _dml.bindStatement();

        ResultColumnList    deleteSignature = _dml.resultSet.resultColumns;
        for ( int i = 0; i < deleteSignature.size(); i++ )
        {
            ResultColumn    origRC = deleteSignature.elementAt( i );
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

    /** Bind a WHEN NOT MATCHED ... THEN INSERT clause */
    private void    bindInsert
    (
            DataDictionary dd,
            MergeNode  mergeNode,
            FromList fullFromList,
            FromTable targetTable
    )
            throws StandardException
    {
        // the VALUES clause may not mention columns in the target table
        FromList    targetTableFromList = new FromList( getNodeFactory().doJoinOrderOptimization(), getContextManager() );
        targetTableFromList.addElement( fullFromList.elementAt( 0 ) );
        bindExpressions( _insertValues, targetTableFromList );
        if ( _matchingRefinement != null )
        {
            mergeNode.bindExpression( _matchingRefinement, targetTableFromList );
        }

        SelectNode  selectNode = new SelectNode
                (
                        _insertValues,      // select list
                        fullFromList,
                        null,      // where clause
                        null,      // group by list
                        null,      // having clause
                        null,      // window list
                        null,      // optimizer plan override
                        getContextManager()
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
    }

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
     * Calculate the 1-based offsets which define the rows which will be buffered up
     * for this INSERT/UPDATE/DELETE action at run-time. The rows are constructed
     * from the columns in the SELECT list of the driving left joins. This method
     * calculates an array of offsets into the SELECT list. The columns at those
     * offsets will form the row which is buffered up for the INSERT/UPDATE/DELETE
     * action.
     * </p>
     */
    void    bindThenColumns( ResultColumnList selectList )
            throws StandardException
    {
        int     bufferedCount = _thenColumns.size();
        int     selectCount = selectList.size();

        _thenColumnOffsets = new int[ bufferedCount ];

        for ( int bidx = 0; bidx < bufferedCount; bidx++ )
        {
            ResultColumn    bufferedRC = _thenColumns.elementAt( bidx );
            ValueNode       bufferedExpression = bufferedRC.getExpression();
            int                     offset = -1;    // start out undefined

            if ( bufferedExpression.getColumnName().equals("###RowLocationToDelete"))
                offset = selectCount;
            else if ( bufferedExpression instanceof ColumnReference )
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
                                tableName != null && tableName.equals( selectCR.getTableName() ) &&
                                        columnName.equals( selectCR.getColumnName() )
                        )
                        {
                            offset = sidx + 1;
                            break;
                        }
                    }
                }
            }
            else if( bufferedExpression instanceof CurrentRowLocationNode )
            {
                //
                // There is only one RowLocation in the SELECT list, the row location for the
                // tuple from the target table. The RowLocation is always the last column in
                // the SELECT list.
                //
                offset = selectCount;
            }

            _thenColumnOffsets[ bidx ] = offset;
        }
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
                        _thenColumnOffsets,
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
     * Generate a method to invoke the INSERT/UPDATE/DELETE action. This method
     * will be called at runtime by MatchingClauseConstantAction.executeConstantAction().
     * </p>
     */
    void    generate( ActivationClassBuilder acb, int clauseNumber )
            throws StandardException
    {
        _clauseNumber = clauseNumber;
        _actionMethodName = "mergeActionMethod_" + _clauseNumber;

        MethodBuilder mb = acb.getClassBuilder().newMethodBuilder
                (
                        Modifier.PUBLIC,
                        ClassName.ResultSet,
                        _actionMethodName
                );
        mb.addThrownException(ClassName.StandardException);

        // now generate the action into this method
        _dml.generate( acb, mb );

        mb.methodReturn();
        mb.complete();
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
