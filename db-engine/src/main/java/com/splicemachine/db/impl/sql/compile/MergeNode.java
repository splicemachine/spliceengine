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
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.util.*;

/**
 * This class represents a MERGE INTO statement.
 */
public class MergeNode extends DMLModStatementNode
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
    private QueryTreeNodeVector<MatchingClauseNode> _matchingClauses;

    //
    // Filled in at bind() time.
    //
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

    public MergeNode(FromTable targetTable, FromTable sourceTable, ValueNode searchCondition,
                     QueryTreeNodeVector<MatchingClauseNode> matchingClauses, ContextManager cm)
            throws StandardException
    {
        setContextManager(cm);

        if ( !( targetTable instanceof FromBaseTable) ) { notBaseTable(); }
        else { _targetTable = (FromBaseTable) targetTable; }

        _sourceTable = sourceTable;
        _searchCondition = searchCondition;
        _matchingClauses = matchingClauses;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BIND-TIME ENTRY POINTS CALLED BY MatchingClauseNode
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the target table for the MERGE statement */
    FromBaseTable   getTargetTable() { return _targetTable; }

    /**
     * <p>
     * Associate a column with the SOURCE or TARGET table. This is
     * part of the special name resolution machinery which smooths over
     * the differences between name resolution for SELECTs and UPDATEs.
     * </p>
     */
    void associateColumn( FromList fromList, ColumnReference cr, int mergeTableID )
            throws StandardException
    {
        if ( mergeTableID != ColumnReference.MERGE_UNKNOWN)
        {
            cr.setMergeTableID( mergeTableID );
        }
        else
        {
            // we have to figure out which table the column is in
            String  columnsTableName = cr.getTableName();

            if ( ((FromTable) fromList.elementAt( SOURCE_TABLE_INDEX )).getMatchingColumn( cr ) != null )
            {
                cr.setMergeTableID( ColumnReference.MERGE_SOURCE );
            }
            else if ( ((FromTable) fromList.elementAt( TARGET_TABLE_INDEX )).getMatchingColumn( cr ) != null )
            {
                cr.setMergeTableID( ColumnReference.MERGE_TARGET );
            }
        }

        // Don't raise an error if a column in another table is referenced and we
        // don't know how to handle it here. If the column is not in the SOURCE or TARGET
        // table, then it will be caught by other bind-time logic. Columns which ought
        // to be associated, but aren't, will be caught later on by MatchingClauseNode.getMergeTableID().
    }

    /** Boilerplate for binding an expression against a FromList */
    void bindExpression( ValueNode value, FromList fromList )
            throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();

        cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
        cc.pushCurrentPrivType( Authorizer.SELECT_PRIV );

        try {
            // this adds SELECT priv on referenced columns and EXECUTE privs on referenced routines
            value.bindExpression(fromList, new SubqueryList( getContextManager() ), new ArrayList<AggregateNode>());
        }
        finally
        {
            // Restore previous compiler state
            cc.popCurrentPrivType();
            cc.setReliability( previousReliability );
        }
    }

    /**
     * <p>
     * Add the columns in the matchingRefinement clause to the evolving map.
     * This is called when we're building the SELECT list for the driving left join.
     * </p>
     */
    void getColumnsInExpression( HashMap<String,ColumnReference> map, ValueNode expression, int mergeTableID )
            throws StandardException
    {
        if ( expression == null ) { return; }

        List<ColumnReference> colRefs = getColumnReferences( expression );

        getColumnsFromList( map, colRefs, mergeTableID );
    }

    /**
     * <p>
     * Add a list of columns to the the evolving map.
     * This is called when we're building the SELECT list for the driving left join.
     * </p>
     */
    void getColumnsFromList( HashMap<String,ColumnReference> map, ResultColumnList rcl, int mergeTableID )
            throws StandardException
    {
        List<ColumnReference> colRefs = getColumnReferences( rcl );

        getColumnsFromList( map, colRefs, mergeTableID );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //@Override
    public void bindStatement() throws StandardException
    {
        DataDictionary dd = getDataDictionary();

        // source table must be a vti or base table
        if ( !(_sourceTable instanceof FromVTI) && !(_sourceTable instanceof FromBaseTable) )
        {

            throw new RuntimeException("VTI or Table");
            //throw StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_OR_VTI );
        }

        // source and target may not have the same correlation names
//        if ( getExposedName( _targetTable ).equals( getExposedName( _sourceTable ) ) )
//        {
//            throw StandardException.newException( SQLState.LANG_SAME_EXPOSED_NAME );
//        }

        // don't allow derived column lists right now
//        forbidDerivedColumnLists();

        // synonyms not allowed
//        forbidSynonyms();

        //
        // Don't add any privileges until we bind the matching clauses.
        //
//        IgnoreFilter    ignorePermissions = new IgnoreFilter();
//        getCompilerContext().addPrivilegeFilter( ignorePermissions );

        FromList      dfl = new FromList( getContextManager() );
        FromTable     dflSource = cloneFromTable( _sourceTable );
        FromBaseTable dflTarget = (FromBaseTable) cloneFromTable( _targetTable );

        dfl.addFromTable( dflSource );
        dfl.addFromTable( dflTarget );
        // todo: doJoinOrderOptimization
        dfl.bindTables( dd, new FromList( /*getOptimizerFactory().doJoinOrderOptimization(),*/ getContextManager() ) );

        // target table must be a base table
        if ( !targetIsBaseTable( dflTarget ) ) { notBaseTable(); }

        // ready to add permissions
        // todo
        //getCompilerContext().removePrivilegeFilter( ignorePermissions );

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            FromList    dummyFromList = cloneFromList( dd, dflTarget );
            FromBaseTable   dummyTargetTable = (FromBaseTable) dummyFromList.elementAt( TARGET_TABLE_INDEX );
            // todo
            //mcn.bind( dd, this, dummyFromList, dummyTargetTable );

            // window function not allowed
            SelectNode.checkNoWindowFunctions(mcn, "matching clause");

            // aggregates not allowed
            checkNoAggregates(mcn);
        }

        // todo
        //bindLeftJoin( dd );
    }


    static void checkNoAggregates(QueryTreeNode clause)
            throws StandardException {

        // Clause cannot contain window aggregates except inside subqueries
        HasNodeVisitor visitor = new HasNodeVisitor(AggregateNode.class, SubqueryNode.class);
        clause.accept(visitor);

        if (visitor.hasNode()) {
            throw new RuntimeException("LANG_NO_AGGREGATES_IN_MERGE_MATCHING_CLAUSE");
//            throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_MERGE_MATCHING_CLAUSE);
        }
    }

    /////////////////////////////////////
    //
    // TABLE AND CORRELATION CHECKS
    //
    /////////////////////////////////////

    /** Get the exposed name of a FromTable */
    private String getExposedName( FromTable ft ) throws StandardException
    {
        return ft.getTableName().getTableName();
    }
    /**
     *<p>
     * Because of name resolution complexities, we do not allow derived column lists
     * on source or target tables. These lists arise in queries like the following:
     * </p>
     *
     * <pre>
     * merge into t1 r( x )
     * using t2 on r.x = t2.a
     * when matched then delete;
     *
     * merge into t1
     * using t2 r( x ) on t1.a = r.x
     * when matched then delete;
     * </pre>
     */
    private void    forbidDerivedColumnLists() throws StandardException
    {
        if ( (_sourceTable.getResultColumns() != null) || (_targetTable.getResultColumns() != null) )
        {
            throw new RuntimeException("LANG_NO_DCL_IN_MERGE");
            //throw StandardException.newException( SQLState.LANG_NO_DCL_IN_MERGE );
        }
    }

    /** Neither the source nor the target table may be a synonym */
    private void forbidSynonyms() throws StandardException
    {
        forbidSynonyms(_targetTable.getTableNameField().cloneMe());
        if ( _sourceTable instanceof FromBaseTable )
        {
            forbidSynonyms(
                    ((FromBaseTable) _sourceTable).getTableNameField().cloneMe());
        }
    }

    private void forbidSynonyms(TableName tableName) throws StandardException
    {
        tableName.bind();

        TableName   synonym = resolveTableToSynonym( tableName );
        if ( synonym != null )
        {
            throw new RuntimeException("LANG_NO_SYNONYMS_IN_MERGE");
            //throw StandardException.newException( SQLState.LANG_NO_SYNONYMS_IN_MERGE );
        }
    }

    /** Throw a "not base table" exception */
    private void notBaseTable()  throws StandardException
    {
        throw new RuntimeException("LANG_TARGET_NOT_BASE_TABLE");
        //throw StandardException.newException( SQLState.LANG_TARGET_NOT_BASE_TABLE );
    }

    /** Return true if the target table is a base table */
    private boolean targetIsBaseTable( FromBaseTable targetTable ) throws StandardException
    {
        FromBaseTable   fbt = targetTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        switch( desc.getTableType() )
        {
            case TableDescriptor.BASE_TABLE_TYPE:
            case TableDescriptor.LOCAL_TEMPORARY_TABLE_TYPE: // todo: check if this is really LOCAL, not GLOBAL
                return true;

            default:
                return false;
        }
    }

    /** Return true if the source table is a base table, view, or table function */
    private boolean sourceIsBase_or_VTI() throws StandardException
    {
        if ( _sourceTable instanceof FromVTI ) { return true; }
        if ( !( _sourceTable instanceof FromBaseTable) ) { return false; }

        FromBaseTable   fbt = (FromBaseTable) _sourceTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        switch( desc.getTableType() )
        {
            case TableDescriptor.BASE_TABLE_TYPE:
            case TableDescriptor.SYSTEM_TABLE_TYPE:
            case TableDescriptor.LOCAL_TEMPORARY_TABLE_TYPE: // todo: check if this is really LOCAL, not GLOBAL
                return true;

            default:
                return false;
        }
    }

    ///////////////////////////
    //
    // BINDING THE LEFT JOIN
    //
    ///////////////////////////

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

            //
            // Don't add any privileges until we bind the matching refinement clauses.
            //
            // todo
//            IgnoreFilter    ignorePermissions = new IgnoreFilter();
//            getCompilerContext().addPrivilegeFilter( ignorePermissions );

            _hojn = new HalfOuterJoinNode(_sourceTable, _targetTable, _searchCondition, null, false, null, getContextManager());

            _leftJoinFromList = _hojn.makeFromList( true, true );
            // todo
            _leftJoinFromList.bindTables( dd, new FromList( /*getOptimizerFactory().doJoinOrderOptimization(), */ getContextManager() ) );

            if ( !sourceIsBase_or_VTI() )
            {
                throw new RuntimeException("LANG_SOURCE_NOT_BASE_OR_VTI");
                //throw StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_OR_VTI );
            }

            FromList    topFromList = new FromList( /*getOptimizerFactory().doJoinOrderOptimization(),*/ getContextManager() );
            topFromList.addFromTable( _hojn );

            // ready to add permissions
            // todo
            //getCompilerContext().removePrivilegeFilter( ignorePermissions );

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

            ResultColumnList selectList = buildSelectList();

            // save a copy so that we can remap column references when generating the temporary rows
            _selectList = selectList.copyListAndObjects();

            resultSet = new SelectNode
                    (
                            selectList,
                            topFromList,
                            null,      // where clause
                            null,      // group by list
                            null,      // having clause
                            null,      // window list
                            null,      // optimizer plan override
                            getContextManager()
                    );

            // Wrap the SELECT in a CursorNode in order to finish binding it.
            _leftJoinCursor = new CursorNode
                    (
                            "SELECT",
                            resultSet,
                            null,
                            null,
                            null,
                            null,
                            false,
                            CursorNode.READ_ONLY,
                            null,
                            true,
                            getContextManager()
                    );

            //
            // We're only interested in privileges related to the ON clause.
            // Otherwise, the driving left join should not contribute any
            // privilege requirements.
            //
            // todo
            //getCompilerContext().addPrivilegeFilter( ignorePermissions );

            _leftJoinCursor.bindStatement();

            // ready to add permissions again
            // todo
            //getCompilerContext().removePrivilegeFilter( ignorePermissions );

            // now figure out what privileges are needed for the ON clause
            addOnClausePrivileges();
        }
        finally
        {
            // Restore previous compiler state
            cc.setReliability( previousReliability );
        }
    }

    ////////////////////////////
    //
    // CLONING THE FROM LIST
    //
    ////////////////////////////

    /** Create a FromList for binding a WHEN [ NOT ] MATCHED clause */
    private FromList cloneFromList( DataDictionary dd, FromBaseTable targetTable )
            throws StandardException
    {
        FromList      dummyFromList = new FromList( getContextManager() );
        FromBaseTable dummyTargetTable = new FromBaseTable( targetTable.getTableNameField(), targetTable.correlationName,
                                                            null, null, getContextManager() );
        FromTable     dummySourceTable = cloneFromTable( _sourceTable );

        dummyTargetTable.setMergeTableID( ColumnReference.MERGE_TARGET );
        dummySourceTable.setMergeTableID ( ColumnReference.MERGE_SOURCE );

        dummyFromList.addFromTable( dummySourceTable );
        dummyFromList.addFromTable( dummyTargetTable );

        //
        // Don't add any privileges while binding the tables.
        //
        // todo
//        IgnoreFilter    ignorePermissions = new IgnoreFilter();
//        getCompilerContext().addPrivilegeFilter( ignorePermissions );

        // todo
        dummyFromList.bindTables( dd, new FromList( /*getOptimizerFactory().doJoinOrderOptimization(),*/ getContextManager() ) );

        // ready to add permissions
        // todo
        //getCompilerContext().removePrivilegeFilter( ignorePermissions );

        return dummyFromList;
    }

    /** Clone a FromTable to avoid binding the original */
    private FromTable cloneFromTable( FromTable fromTable ) throws StandardException
    {
        if( fromTable instanceof FromVTI )
        {
            FromVTI source = (FromVTI) fromTable;

            return new FromVTI(source.methodCall, source.correlationName, source.getResultColumns(), null,
                    source.exposedName, null /* type descriptor */, getContextManager());
        }
        else if( fromTable instanceof FromBaseTable )
        {
            FromBaseTable source = (FromBaseTable) fromTable;
            return new FromBaseTable(source.tableName, source.correlationName, null, null, getContextManager());
        }
        else
        {
            throw new RuntimeException("LANG_SOURCE_NOT_BASE_OR_VTI");
        }
    }

    ///////////////////////////
    //
    // PRIVILEGE MANAGEMENT
    //
    ///////////////////////////

    /**
     * <p>
     * Add the privileges required by the ON clause.
     * </p>
     */
    private void addOnClausePrivileges() throws StandardException
    {
        // todo
    }

    /**
     * <p>
     * Add SELECT privilege on the indicated column.
     * </p>
     */
    private void    addColumnPrivilege( ColumnReference cr )
            throws StandardException
    {
        // todo
    }

    /**
     * <p>
     * Add EXECUTE privilege on the indicated routine.
     * </p>
     */
    private void addRoutinePrivilege( StaticMethodCallNode routine )
            throws StandardException
    {
        // todo
    }
    //////////////////

    /** Get a list of CastNodes in an expression */
    private List<CastNode> getCastNodes( QueryTreeNode expression )
            throws StandardException
    {
        CollectNodesVisitorT<CastNode> getCNs = new CollectNodesVisitorT<>(CastNode.class);
        expression.accept(getCNs);
        return getCNs.getList();
    }

    /** Get a list of routines in an expression */
    private List<StaticMethodCallNode> getRoutineReferences( QueryTreeNode expression )
            throws StandardException
    {
        CollectNodesVisitor getSMCNs = new CollectNodesVisitor(StaticMethodCallNode.class);
        expression.accept(getSMCNs);
        return getSMCNs.getList();
    }

////////////////////////////
    ///////////////////////////////
    //
    // BUILD THE SELECT LIST
    // FOR THE DRIVING LEFT JOIN.
    //
    ///////////////////////////////

    /** Build the select list for the left join */
    private ResultColumnList    buildSelectList() throws StandardException
    {
        HashMap<String,ColumnReference> drivingColumnMap = new HashMap<String,ColumnReference>();
        getColumnsInExpression( drivingColumnMap, _searchCondition, ColumnReference.MERGE_UNKNOWN );

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.getColumnsInExpressions( this, drivingColumnMap );

            int mergeTableID = mcn.isDeleteClause() ? ColumnReference.MERGE_TARGET : ColumnReference.MERGE_UNKNOWN;
            getColumnsFromList( drivingColumnMap, mcn.getThenColumns(), mergeTableID );
        }

        ResultColumnList    selectList = new ResultColumnList( getContextManager() );

        // add all of the columns from the source table which are mentioned
        addColumns
                (
                        (FromTable) _leftJoinFromList.elementAt( SOURCE_TABLE_INDEX ),
                        drivingColumnMap,
                        selectList,
                        ColumnReference.MERGE_SOURCE
                );
        // add all of the columns from the target table which are mentioned
        addColumns
                (
                        (FromTable) _leftJoinFromList.elementAt( TARGET_TABLE_INDEX ),
                        drivingColumnMap,
                        selectList,
                        ColumnReference.MERGE_TARGET
                );

        addTargetRowLocation( selectList );

        return selectList;
    }

    /** Add the target table's row location to the left join's select list */
    private void    addTargetRowLocation( ResultColumnList selectList )
            throws StandardException
    {
        // tell the target table to generate a row location column
        _targetTable.setRowLocationColumnName( TARGET_ROW_LOCATION_NAME );

        TableName   fromTableName = _targetTable.getTableName();
        ColumnReference cr = new ColumnReference( TARGET_ROW_LOCATION_NAME, fromTableName, getContextManager() );
        cr.setMergeTableID( ColumnReference.MERGE_TARGET );
        ResultColumn    rowLocationColumn = new ResultColumn( (String) null, cr, getContextManager() );
        rowLocationColumn.markGenerated();

        selectList.addResultColumn( rowLocationColumn );
    }

    /**
     * <p>
     * Add to an evolving select list the columns from the indicated table.
     * </p>
     */
    private void    addColumns
    (
            FromTable  fromTable,
            HashMap<String,ColumnReference> drivingColumnMap,
            ResultColumnList   selectList,
            int    mergeTableID
    )
            throws StandardException
    {
        String[]    columnNames = getColumns( mergeTableID, drivingColumnMap );
        TableName   tableName = fromTable.getTableName();

        for ( int i = 0; i < columnNames.length; i++ )
        {
            ColumnReference cr = new ColumnReference
                    ( columnNames[ i ], tableName, getContextManager() );
            cr.setMergeTableID( mergeTableID );
            ResultColumn    rc = new ResultColumn( (String) null, cr, getContextManager() );
            selectList.addResultColumn( rc );
        }
    }

    /** Get the column names from the table with the given table number, in sorted order */
    private String[]    getColumns( int mergeTableID, HashMap<String,ColumnReference> map )
    {
        HashSet<String> set = new HashSet<String>();

        for ( ColumnReference cr : map.values() )
        {
            if ( cr.getMergeTableID() == mergeTableID ) { set.add( cr.getColumnName() ); }
        }

        String[]    retval = new String[ set.size() ];
        set.toArray( retval );
        Arrays.sort( retval );

        return retval;
    }

    /** Get a list of column references in an expression */
    private List<ColumnReference>   getColumnReferences( QueryTreeNode expression )
            throws StandardException
    {
        CollectNodesVisitorT<ColumnReference> getCRs =  new CollectNodesVisitorT<>(ColumnReference.class);
        expression.accept(getCRs);
        return getCRs.getList();
    }

    /** Add a list of columns to the the evolving map */
    private void getColumnsFromList( HashMap<String,ColumnReference> map,
                                     List<ColumnReference> colRefs, int mergeTableID )
            throws StandardException
    {
        for ( ColumnReference cr : colRefs )
        {
            addColumn( map, cr, mergeTableID );
        }
    }

    /** Add a column to the evolving map of referenced columns */
    void addColumn(HashMap<String,ColumnReference> map, ColumnReference cr, int mergeTableID)
            throws StandardException
    {
        if ( cr.getTableName() == null )
        {
            cr = cr.bindExpression(_leftJoinFromList, new SubqueryList(getContextManager()), new ArrayList<AggregateNode>());
            TableName       tableName = cr.getQualifiedTableName();
            cr = new ColumnReference( cr.getColumnName(), tableName, getContextManager() );
        }

        associateColumn( _leftJoinFromList, cr, mergeTableID );

        String  key = makeDCMKey( cr.getTableName(), cr.getColumnName() );

        ColumnReference mapCR = map.get( key );
        if ( mapCR != null )
        {
            mapCR.setMergeTableID( cr.getMergeTableID() );
        }
        else
        {
            map.put( key, cr );
        }
    }

    /** Make a HashMap key for a column in the driving column map of the LEFT JOIN */
    private String  makeDCMKey( String tableName, String columnName )
    {
        return IdUtil.mkQualifiedName( tableName, columnName );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // optimize() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void optimizeStatement() throws StandardException
    {
        //
        // Don't add any privileges during optimization.
        //
        // todo
//        IgnoreFilter    ignorePermissions = new IgnoreFilter();
//        getCompilerContext().addPrivilegeFilter( ignorePermissions );

        /* First optimize the left join */
        _leftJoinCursor.optimizeStatement();

        //
        // No need to set lockMode in the master MergeNode. The individual
        // actions and the driving left-join will set their own lock modes.
        //

        // now optimize the INSERT/UPDATE/DELETE actions
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.optimize();
        }

        // ready to add permissions again
        // todo
        // getCompilerContext().removePrivilegeFilter( ignorePermissions );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // generate() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb)
            throws StandardException
    {
        int     clauseCount = _matchingClauses.size();

        /* generate the parameters */
        generateParameterValueSet(acb);

        acb.pushGetResultSetFactoryExpression( mb );

        // arg 1: the driving left join
        _leftJoinCursor.generate( acb, mb );

        // dig up the actual result set which was generated and which will drive the MergeResultSet
        ScrollInsensitiveResultSetNode  sirs = (ScrollInsensitiveResultSetNode) _leftJoinCursor.resultSet;
        ResultSetNode   generatedScan = sirs.getChildResult();

        ConstantAction[]    clauseActions = new ConstantAction[ clauseCount ];
        for ( int i = 0; i < clauseCount; i++ )
        {
            MatchingClauseNode mcn = _matchingClauses.elementAt(i);

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

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth		The depth of this node in the tree
     */
    @Override
    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes( depth );

            printLabel( depth, "targetTable: " );
            _targetTable.treePrint( depth + 1 );

            printLabel( depth, "sourceTable: " );
            _sourceTable.treePrint( depth + 1 );

            if ( _searchCondition != null )
            {
                printLabel( depth, "searchCondition: " );
                _searchCondition.treePrint( depth + 1 );
            }

            for ( MatchingClauseNode mcn : _matchingClauses )
            {
                printLabel( depth, mcn.toString() );
                mcn.treePrint( depth + 1 );
            }
        }
    }

    @Override
    public String statementToString()
    {
        return "MERGE";
    }
}
