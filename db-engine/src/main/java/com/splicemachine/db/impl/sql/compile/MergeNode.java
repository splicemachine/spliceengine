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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Node;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ResultSetFactory;
import com.splicemachine.db.iapi.util.IdUtil;
import org.apache.avro.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * <p>
 * A MergeNode represents a MERGE statement. The statement looks like
 * this...
 * </p>
 *
 * <pre>
 * MERGE INTO targetTable
 * USING sourceTable
 * ON searchCondition
 * matchingClause1 ... matchingClauseN
 * </pre>
 *
 * <p>
 * ...where each matching clause looks like this...
 * </p>
 *
 * <pre>
 * WHEN MATCHED [ AND matchingRefinement ] THEN DELETE
 * </pre>
 *
 * <p>
 * ...or
 * </p>
 *
 * <pre>
 * WHEN MATCHED [ AND matchingRefinement ] THEN UPDATE SET col1 = expr1, ... colM = exprM
 * </pre>
 *
 * <p>
 * ...or
 * </p>
 *
 * <pre>
 * WHEN NOT MATCHED [ AND matchingRefinement ] THEN INSERT columnList VALUES valueList
 * </pre>
 *
 * <p>
 * The Derby compiler essentially rewrites this statement into a driving left join
 * followed by a series of DELETE/UPDATE/INSERT actions. The left join looks like
 * this:
 * </p>
 *
 * <pre>
 * SELECT selectList FROM sourceTable LEFT OUTER JOIN targetTable ON searchCondition
 * </pre>
 *
 * <p>
 * The selectList of the driving left join consists of the following:
 * </p>
 *
 * <ul>
 * <li>All of the columns mentioned in the searchCondition.</li>
 * <li>All of the columns mentioned in the matchingRefinement clauses.</li>
 * <li>All of the columns mentioned in the SET clauses and the INSERT columnLists and valueLists.</li>
 * <li>All additional columns needed for the triggers and foreign keys fired by the DeleteResultSets
 * and UpdateResultSets constructed for the WHEN MATCHED clauses.</li>
 * <li>All additional columns needed to build index rows and evaluate generated columns
 * needed by the UpdateResultSets constructed for the WHEN MATCHED...THEN UPDATE clauses.</li>
 * <li>A trailing targetTable.RowLocation column.</li>
 * </ul>
 *
 * <p>
 * The matchingRefinement expressions are bound and generated against the
 * FromList of the driving left join. Dummy DeleteNode, UpdateNode, and InsertNode
 * statements are independently constructed in order to bind and generate the DELETE/UPDATE/INSERT
 * actions.
 * </p>
 *
 * <p>
 * At execution time, the targetTable.RowLocation column is used to determine
 * whether a given driving row matches. The row matches iff targetTable.RowLocation is not null.
 * The driving row is then assigned to the
 * first DELETE/UPDATE/INSERT action to which it applies. The relevant columns from
 * the driving row are extracted and buffered in a temporary table specific to that
 * DELETE/UPDATE/INSERT action. After the driving left join has been processed,
 * the DELETE/UPDATE/INSERT actions are run in order, each taking its corresponding
 * temporary table as its source ResultSet.
 * </p>
 */
public class MergeNode extends DMLStatementNode
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   int SOURCE_TABLE_INDEX = 0;
    public  static  final   int TARGET_TABLE_INDEX = 1;

    private static  final   String  TARGET_ROW_LOCATION_NAME = "###TargetRowLocation";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //
    // Filled in by the constructor.
    //
    private FromBaseTable   _targetTable;
    private FromTable   _sourceTable;
    private ValueNode   _searchCondition;
    private List<MatchingClauseNode>   _matchingClauses;

    // filled in at bind() time
    private ResultColumnList    _selectList;
    private FromList                _leftJoinFromList;
    private HalfOuterJoinNode   _hojn;

    //
    // Filled in at generate() time.
    //
    private ConstantAction      _constantAction;
    private CursorNode          _leftJoinCursor;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Constructor for a MergeNode.
     * </p>
     */
    public  MergeNode
    (
            FromTable          targetTable,
            FromTable          sourceTable,
            ValueNode          searchCondition,
            QueryTreeNodeVector<MatchingClauseNode> matchingClauses,
            ContextManager     cm
    )
            throws StandardException
    {
        setContextManager(cm);
        //super( null, null, cm );

        if ( !( targetTable instanceof FromBaseTable) ) { notBaseTable(); }
        else { _targetTable = (FromBaseTable) targetTable; }

        _sourceTable = sourceTable;
        _searchCondition = searchCondition;
        _matchingClauses = matchingClauses.getNodes();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void bindStatement() throws StandardException
    {
        DataDictionary  dd = getDataDictionary();

        FromList    dummyFromList = new FromList( getContextManager() );
        FromBaseTable   dummyTargetTable = new FromBaseTable
                (
                        _targetTable.tableName,
                        _targetTable.correlationName,
                        null,
                        null,
                        getContextManager()
                );
        FromTable       dummySourceTable = cloneSourceTable();

        // source and target may not have the same correlation names
        if ( getExposedName( dummyTargetTable ).equals( getExposedName( dummySourceTable ) ) )
        {
            throw new RuntimeException("StandardException.newException( SQLState.LANG_SAME_EXPOSED_NAME )");
        }

        dummyFromList.addFromTable( dummySourceTable );
        dummyFromList.addFromTable( dummyTargetTable );
        dummyFromList.bindTables( dd, new FromList( getNodeFactory().doJoinOrderOptimization(), getContextManager() ) );

        if ( !targetIsBaseTable( dummyTargetTable ) ) { notBaseTable(); }

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.bind( dd, this, dummyFromList, dummyTargetTable );
        }

        bindLeftJoin( dd );

        // re-bind the matchingRefinement clauses now that we have result set numbers
        // from the driving left join.
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.bindResultSetNumbers( this, _leftJoinFromList );
        }
    }

    /** Get the exposed name of a FromTable */
    private String  getExposedName( FromTable ft ) throws StandardException
    {
        return ft.getTableName().getTableName();
    }

    /**
     * Bind the driving left join select.
     * Stuffs the left join SelectNode into the resultSet variable.
     */
    private void    bindLeftJoin( DataDictionary dd )   throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();

        try {
            cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );

            _hojn = new HalfOuterJoinNode
                    (
                            _sourceTable,      // leftResult
                            _targetTable,      // rightResult
                            _searchCondition,  // onClause
                            null,              // usingClause
                            false,             // rightOuterJoin
                            null,              // tableProperties
                            getContextManager()
                    );

            _leftJoinFromList = _hojn.makeFromList( true, true );
            _leftJoinFromList.bindTables( dd, new FromList( getNodeFactory().doJoinOrderOptimization(), getContextManager() ) );

            if ( !sourceIsBase_View_or_VTI() )
            {
                throw new RuntimeException("StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_VIEW_OR_VTI )");
            }

            FromList    topFromList = new FromList( getNodeFactory().doJoinOrderOptimization(), getContextManager() );
            topFromList.addFromTable( _hojn );

            // preliminary binding of the matching clauses to resolve column
            // references. this ensures that we can add all of the columns from
            // the matching refinements to the SELECT list of the left join.
            // we re-bind the matching clauses when we're done binding the left join
            // because, at that time, we have result set numbers needed for
            // code generation.
            for ( MatchingClauseNode mcn : _matchingClauses )
            {
                mcn.bindRefinement( this, _leftJoinFromList );
            }

            ResultColumnList    selectList = buildSelectList();

            // save a copy so that we can remap column references when generating the temporary rows
            _selectList = selectList.copyListAndObjects();

            // calculate the offsets into the SELECT list which define the rows for
            // the WHEN [ NOT ] MATCHED  actions
            for ( MatchingClauseNode mcn : _matchingClauses )
            {
                mcn.bindThenColumns( _selectList );
            }

            resultSet = new SelectNode(selectList, topFromList, null, getContextManager());

            // Wrap the SELECT in a CursorNode in order to finish binding it.
            _leftJoinCursor = new CursorNode
                    (
                            "SELECT",  // statementType
                            resultSet, // resultSet
                            null, null, null, null,
                            false,     // hasJDBClimitClause
                            CursorNode.READ_ONLY, // updateMode
                            null,
                            getContextManager()
                    );
            _leftJoinCursor.bindStatement();
        }
        finally
        {
            // Restore previous compiler state
            cc.setReliability( previousReliability );
        }
    }

    /** Get the target table for the MERGE statement */
    FromBaseTable   getTargetTable() { return _targetTable; }

    /** Throw a "not base table" exception */
    private void    notBaseTable()  throws StandardException
    {
        throw new RuntimeException("throw StandardException.newException( SQLState.LANG_TARGET_NOT_BASE_TABLE )");
    }

    /** Build the select list for the left join */
    private ResultColumnList    buildSelectList() throws StandardException
    {
        HashMap<String,ColumnReference> drivingColumnMap = new HashMap<String,ColumnReference>();
        getColumnsInExpression( drivingColumnMap, _searchCondition );

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.getColumnsInExpressions( this, drivingColumnMap );
            getColumnsFromList( drivingColumnMap, mcn.getBufferedColumns() );
        }

        ResultColumnList    selectList = new ResultColumnList( getContextManager() );

        // add all of the columns from the source table which are mentioned
        addColumns( (FromTable) _leftJoinFromList.elementAt( SOURCE_TABLE_INDEX ), drivingColumnMap, selectList );
        // add all of the columns from the target table which are mentioned
        addColumns( (FromTable) _leftJoinFromList.elementAt( TARGET_TABLE_INDEX ), drivingColumnMap, selectList );

        addTargetRowLocation( selectList );

        return selectList;
    }

    boolean IsRowLocation(ValueNode node) {
        String lastColName = node.getColumnName();
        return (lastColName.equals(UpdateNode.COLUMNNAME) || lastColName.equals(DeleteNode.COLUMNNAME));
    }

    /** Add the target table's row location to the left join's select list */
    private void    addTargetRowLocation( ResultColumnList selectList )
            throws StandardException
    {
        if(selectList.size() > 0) {
            if( IsRowLocation( selectList.elementAt(selectList.size() - 1).getExpression() ))
            {
                // last column is already RowLocation, no need to add one
                return;
            }
        }
        // tell the target table to generate a row location column
        _targetTable.setRowLocationColumnName( TARGET_ROW_LOCATION_NAME );

        TableName   fromTableName = _targetTable.getTableName();
        ColumnReference cr = new ColumnReference
                ( TARGET_ROW_LOCATION_NAME, fromTableName, getContextManager() );
        ResultColumn    rowLocationColumn = new ResultColumn( (String) null, cr, getContextManager() );
        rowLocationColumn.markGenerated();

        selectList.addResultColumn( rowLocationColumn );
    }

    /** Return true if the target table is a base table */
    private boolean targetIsBaseTable( FromBaseTable targetTable ) throws StandardException
    {
        FromBaseTable   fbt = targetTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        return ( desc.getTableType() == TableDescriptor.BASE_TABLE_TYPE );
    }

    /** Return true if the source table is a base table, view, or table function */
    private boolean sourceIsBase_View_or_VTI() throws StandardException
    {
        if ( _sourceTable instanceof FromVTI ) { return true; }
        if ( !( _sourceTable instanceof FromBaseTable) ) { return false; }

        FromBaseTable   fbt = (FromBaseTable) _sourceTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        switch( desc.getTableType() )
        {
            case TableDescriptor.BASE_TABLE_TYPE:
            case TableDescriptor.VIEW_TYPE:
                return true;

            default:
                return false;
        }
    }

    /** Clone the source table for binding the MATCHED clauses */
    private FromTable   cloneSourceTable() throws StandardException
    {
        if ( _sourceTable instanceof FromVTI )
        {
            FromVTI source = (FromVTI) _sourceTable;

            return new FromVTI
                    (
                            source.methodCall,
                            source.correlationName,
                            source.resultColumns,
                            null,
                            source.exposedName,
                            getContextManager()
                    );
        }
        else if ( _sourceTable instanceof FromBaseTable )
        {
            FromBaseTable   source = (FromBaseTable) _sourceTable;
            return new FromBaseTable
                    (
                            source.tableName,
                            source.correlationName,
                            null,
                            null,
                            getContextManager()
                    );
        }
        else
        {
            throw new RuntimeException("StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_VIEW_OR_VTI )");
        }
    }

    /**
     * <p>
     * Add to an evolving select list the columns from the indicated table.
     * </p>
     */
    private void addColumns(FromTable fromTable, HashMap<String,ColumnReference> drivingColumnMap,
                            ResultColumnList selectList) throws StandardException
    {
        String[] columnNames = getColumns( getExposedName( fromTable ), drivingColumnMap );

        for ( String columnName : columnNames )
        {
            ColumnReference cr = new ColumnReference( columnName, fromTable.getTableName(), getContextManager() );
            ResultColumn rc = new ResultColumn( (String) null, cr, getContextManager() );
            selectList.addResultColumn( rc );
        }
    }

    /** Get the column names from the table with the given table number, in sorted order */
    private String[] getColumns( String exposedName, HashMap<String,ColumnReference> map )
    {
        ArrayList<String> list = new ArrayList<String>();
        ArrayList<String> rlColumns = new ArrayList<String>();

        // todo: we can have here update AND delete RowLocation columns, do we need both?
        for ( ColumnReference cr : map.values() )
        {
            if ( !exposedName.equals( cr.getTableName() ) )
                continue;
            if( IsRowLocation(cr) )
                rlColumns.add( cr.getColumnName() );
            else
                list.add( cr.getColumnName() );

        }

        // make sure RowLocation are LAST column
        list.addAll( rlColumns );
        return list.toArray( new String[ list.size() ] );
    }

    /** Add the columns in the matchingRefinement clause to the evolving map */
    void getColumnsInExpression( HashMap<String,ColumnReference> map, ValueNode expression )
            throws StandardException
    {
        if ( expression == null ) { return; }

        CollectNodesVisitor getCRs = new CollectNodesVisitor(ColumnReference.class);

        expression.accept(getCRs);
        List<ColumnReference> colRefs = getCRs.getList();

        getColumnsFromList( map, colRefs );
    }

    /** Add a list of columns to the the evolving map */
    private void getColumnsFromList( HashMap<String,ColumnReference> map, ResultColumnList rcl )
            throws StandardException
    {
        CollectNodesVisitor getCRs = new CollectNodesVisitor( ColumnReference.class );

        rcl.accept( getCRs );
        List<ColumnReference> colRefs = getCRs.getList();

        getColumnsFromList( map, colRefs );
    }

    /** Add a list of columns to the the evolving map */
    private void getColumnsFromList( HashMap<String,ColumnReference> map, List<ColumnReference> colRefs )
            throws StandardException
    {
        for ( ColumnReference cr : colRefs )
        {
            addColumn( map, cr );
        }
    }

    /** Add a column to the evolving map of referenced columns */
    void addColumn(HashMap<String,ColumnReference> map, ColumnReference cr)
            throws StandardException
    {
        if ( cr.getTableName() == null )
        {
            ValueNode rc = _leftJoinFromList.bindColumnReference( cr );
            TableName tableName = new TableName( null, rc.getTableName(), getContextManager() );
            cr = new ColumnReference( cr.getColumnName(), tableName, getContextManager() );
        }

        String key = makeDCMKey( cr.getTableName(), cr.getColumnName() );
        if ( map.get( key ) == null )
        {
            map.put( key, cr );
        }
    }

    /** Make a HashMap key for a column in the driving column map of the LEFT JOIN */
    private String makeDCMKey( String tableName, String columnName )
    {
        return IdUtil.mkQualifiedName( tableName, columnName );
    }

    /** Boilerplate for binding an expression against a FromList */
    void bindExpression( ValueNode value, FromList fromList )
            throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();

        try {
            cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );

            value.bindExpression(fromList,
                    new SubqueryList( getContextManager() ),
                    new ArrayList<AggregateNode>() );
        }
        finally
        {
            // Restore previous compiler state
            cc.setReliability( previousReliability );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // optimize() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void optimizeStatement() throws StandardException
    {
        /* First optimize the left join */
        _leftJoinCursor.optimizeStatement();

        /* In language we always set it to row lock, it's up to store to
         * upgrade it to table lock.  This makes sense for the default read
         * committed isolation level and update lock.  For more detail, see
         * Beetle 4133.
         */
        //lockMode = TransactionController.MODE_RECORD;

        // now optimize the INSERT/UPDATE/DELETE actions
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.optimize();
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // generate() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void generate( ActivationClassBuilder acb, MethodBuilder mb )
            throws StandardException
    {
        int clauseCount = _matchingClauses.size();

        /* generate the parameters */
        generateParameterValueSet(acb);

        acb.pushGetResultSetFactoryExpression( mb );

        // getMergeResultSet arg 1: the driving left join
        _leftJoinCursor.generate( acb, mb );

        // dig up the actual result set which was generated and which will drive the MergeResultSet
        ScrollInsensitiveResultSetNode  sirs = (ScrollInsensitiveResultSetNode) _leftJoinCursor.resultSet;
        ResultSetNode   generatedScan = sirs.getChildResult();

        ConstantAction[]    clauseActions = new ConstantAction[ clauseCount ];
        for ( int i = 0; i < clauseCount; i++ )
        {
            MatchingClauseNode  mcn = _matchingClauses.get( i );

            mcn.generate( acb, _selectList, generatedScan, _hojn, i );
            clauseActions[ i ] = mcn.makeConstantAction( acb );
        }
        _constantAction = getGenericConstantActionFactory().getMergeConstantAction( clauseActions );

        mb.callMethod( VMOpcode.INVOKEINTERFACE, (String) null, "getMergeResultSet", ClassName.ResultSet, 1 );
    }

    @Override
    public ConstantAction makeConstantAction() throws StandardException
    {
        return _constantAction;
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
        if ( _leftJoinCursor != null )
        {
            _leftJoinCursor.acceptChildren( v );
        }
        else
        {
            super.acceptChildren( v );

            _targetTable.accept( v );
            _sourceTable.accept( v );
            _searchCondition.accept( v );
        }

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.accept( v );
        }
    }

    @Override
    public String statementToString()
    {
        return "MERGE";
    }

    @Override
    public String toString2() {
        StringBuilder sb = new StringBuilder();
        sb.append("MergeNode\n");
        Node.append2(sb, "targetTable", "  ", _targetTable);
        Node.append2(sb, "sourceTable", "  ", _sourceTable);
        Node.append2(sb, "searchCondition", "  ", _searchCondition);
        sb.append("  matchingClauses\n");
        Node.printList(sb, _matchingClauses, "  ");
        return sb.toString();
    }
}
